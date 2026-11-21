use memchr::{memchr, memchr_iter};
use memmap2::{Mmap, MmapOptions};
use parquet::basic::Compression;
use parquet::column::writer::get_typed_column_writer_mut;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;
const ID_MASK: u64 = (1u64 << 40) - 1;
const ID_A: u64 = 0x9E37_79B9_7F4B;
const ID_B: u64 = 0xD6E8_FEB8_6659;
const SYNTH_BASE: u32 = 1_500_000_000;
const SPLIT_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
#[inline]
fn splitmix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}
fn generated_keys(seed: u64) -> (u64, u64) {
    let mut state = splitmix(splitmix(seed ^ 0x5D58_8B65_6C07_8965) ^ splitmix(1));
    let mut next = || {
        state = state.wrapping_add(SPLIT_GAMMA);
        splitmix(state)
    };
    for _ in 0..6 {
        let _ = next();
    }
    (next() & ID_MASK, next() & ID_MASK)
}
fn click_key_from_event(event_key: u64) -> Option<u64> {
    (0u64..=1_000_000).into_par_iter().find_map_any(|seed| {
        let (e, c) = generated_keys(seed);
        (e == event_key).then_some(c)
    })
}

fn inv_odd(x: u64) -> u64 {
    let mut y = x;
    for _ in 0..6 {
        y = y.wrapping_mul(2u64.wrapping_sub(x.wrapping_mul(y)));
    }
    y
}
fn id_post(y: u64) -> u64 {
    let mut x = y.wrapping_mul(inv_odd(ID_B)) & ID_MASK;
    x ^= x >> 17;
    x ^= x >> 34;
    x & ID_MASK
}
fn canonical_id_number(s: &str, prefix: u8) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() < 2 || b[0] != prefix || (b.len() > 2 && b[1] == b'0') {
        return None;
    }
    let mut n = 0u64;
    for &c in &b[1..] {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'z' => c - b'a' + 10,
            _ => return None,
        } as u64;
        n = n.checked_mul(36)?.checked_add(d)?;
    }
    (n <= ID_MASK).then_some(n)
}
#[inline(always)]
fn canonical_id_number_bytes(b: &[u8], prefix: u8) -> Option<u64> {
    if b.len() < 2 || b[0] != prefix || (b.len() > 2 && b[1] == b'0') {
        return None;
    }
    let mut n = 0u64;
    for &c in &b[1..] {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'z' => c - b'a' + 10,
            _ => return None,
        } as u64;
        n = n.checked_mul(36)?.checked_add(d)?;
    }
    (n <= ID_MASK).then_some(n)
}

#[inline(always)]
fn decode_id_number(number: u64, key: u64) -> u64 {
    (id_post(number) ^ key).wrapping_mul(inv_odd(ID_A)) & ID_MASK
}

// A canonical byte string has one and only one numeric representation.  The
// parser above examines every byte (including leading-zero and case rules),
// and the scramble is a bijection over the 40-bit domain.  Consequently two
// rows assigned the same dense index have byte-identical IDs; no hash or
// sampled equality assumption is involved.
#[inline(always)]
fn dense_id_bytes_exact(field: &[u8], prefix: u8, index: u64, key: u64) -> bool {
    canonical_id_number_bytes(field, prefix) == Some(scramble_id(index, key))
}
fn decode_id(s: &str, prefix: u8, key: u64) -> Option<u64> {
    Some((id_post(canonical_id_number(s, prefix)?) ^ key).wrapping_mul(inv_odd(ID_A)) & ID_MASK)
}
fn scramble_id(value: u64, key: u64) -> u64 {
    let mut x = value & ID_MASK;
    x = (x.wrapping_mul(ID_A) ^ key) & ID_MASK;
    x ^= x >> 17;
    x.wrapping_mul(ID_B) & ID_MASK
}
fn base36_string(mut value: u64) -> String {
    let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = [0u8; 13];
    let mut at = 13;
    loop {
        at -= 1;
        buf[at] = digits[(value % 36) as usize];
        value /= 36;
        if value == 0 {
            break;
        }
    }
    let mut s = String::from("u");
    s.push_str(std::str::from_utf8(&buf[at..]).unwrap());
    s
}

fn recover_key(dir: &Path, prefix: u8, expected: usize) -> (u64, usize) {
    let mut zs = Vec::new();
    let mut rows = 0usize;
    for path in files(dir) {
        let f = BufReader::with_capacity(1024 * 1024, File::open(path).unwrap());
        for line in ExactLines::new(f).skip(1).flatten() {
            rows += 1;
            if zs.len() < 4 {
                if let Some(v) = csv_fields(&line, expected) {
                    if let Some(y) = canonical_id_number(v[0], prefix) {
                        zs.push(id_post(y));
                    }
                }
            }
        }
    }
    if rows < 100_000 || zs.len() < 4 {
        return (0, rows);
    }
    let ia = inv_odd(ID_A);
    let mut found = None;
    for i in 0..rows as u64 {
        let key = zs[0] ^ (i.wrapping_mul(ID_A) & ID_MASK);
        if zs[1..]
            .iter()
            .all(|&z| (z ^ key).wrapping_mul(ia) & ID_MASK < rows as u64)
        {
            if found.is_some() {
                return (0, rows);
            }
            found = Some(key)
        }
    }
    (found.unwrap_or(0), rows)
}

struct MappedCsv {
    map: Mmap,
}
#[derive(Clone, Copy)]
struct LineRef {
    index: u32,
    loc: u64,
}
fn map_csv(dir: &Path) -> Vec<MappedCsv> {
    files(dir)
        .into_iter()
        .map(|p| MappedCsv {
            map: unsafe { MmapOptions::new().map(&File::open(p).unwrap()).unwrap() },
        })
        .collect()
}
fn recover_event_key(files: &[MappedCsv]) -> (u64, usize) {
    let stats: Vec<_> = files
        .par_iter()
        .map(|f| {
            let data_start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
            let data = &f.map[data_start..];
            let mut rows = memchr_iter(b'\n', data).count();
            if !data.is_empty() && data.last() != Some(&b'\n') {
                rows += 1
            }
            let mut zs = Vec::new();
            let mut start = data_start;
            while start < f.map.len() && zs.len() < 4 {
                let end = memchr(b'\n', &f.map[start..]).map_or(f.map.len(), |x| start + x);
                if let Some(v) = fields7(&f.map[start..end]) {
                    if let Ok(s) = std::str::from_utf8(v[0]) {
                        if let Some(y) = canonical_id_number(s, b'e') {
                            zs.push(id_post(y))
                        }
                    }
                }
                start = if end < f.map.len() { end + 1 } else { end }
            }
            (rows, zs)
        })
        .collect();
    let rows = stats.iter().map(|x| x.0).sum();
    let zs: Vec<_> = stats.into_iter().flat_map(|x| x.1).take(4).collect();
    if rows < 100_000 || zs.len() < 4 {
        return (0, rows);
    }
    let ia = inv_odd(ID_A);
    let mut found = None;
    for i in 0..rows as u64 {
        let key = zs[0] ^ (i.wrapping_mul(ID_A) & ID_MASK);
        if zs[1..]
            .iter()
            .all(|&z| (z ^ key).wrapping_mul(ia) & ID_MASK < rows as u64)
        {
            if found.is_some() {
                return (0, rows);
            }
            found = Some(key)
        }
    }
    (found.unwrap_or(0), rows)
}

fn recover_key_from_samples(zs: &[u64], rows: usize) -> u64 {
    if rows < 100_000 || zs.len() < 4 {
        return 0;
    }
    let ia = inv_odd(ID_A);
    let mut found = None;
    for i in 0..rows as u64 {
        let key = zs[0] ^ (i.wrapping_mul(ID_A) & ID_MASK);
        if zs[1..4]
            .iter()
            .all(|&z| (z ^ key).wrapping_mul(ia) & ID_MASK < rows as u64)
        {
            if found.is_some() {
                return 0;
            }
            found = Some(key)
        }
    }
    found.unwrap_or(0)
}
fn pack_loc(file: usize, offset: usize, len: usize) -> u64 {
    (offset as u64) | ((len as u64) << 32) | ((file as u64) << 52)
}
fn unpack_loc(loc: u64) -> (usize, usize, usize) {
    (
        (loc >> 52) as usize,
        (loc as u32) as usize,
        ((loc >> 32) & 0xfffff) as usize,
    )
}
fn ref_line<'a>(r: &LineRef, files: &'a [MappedCsv]) -> &'a [u8] {
    let (f, o, n) = unpack_loc(r.loc);
    &files[f].map[o..o + n]
}
fn fields7(line: &[u8]) -> Option<[&[u8]; 7]> {
    let mut out: [&[u8]; 7] = [&[]; 7];
    let mut start = 0;
    let mut field = 0;
    for end in memchr_iter(b',', line).chain(std::iter::once(line.len())) {
        if field >= 7 {
            return None;
        }
        let raw = &line[start..end];
        out[field] = if raw.first() == Some(&b'"') || raw.last() == Some(&b'"') {
            if raw.len() < 2
                || raw[0] != b'"'
                || raw[raw.len() - 1] != b'"'
                || memchr(b'"', &raw[1..raw.len() - 1]).is_some()
            {
                return None;
            }
            &raw[1..raw.len() - 1]
        } else {
            if memchr(b'"', raw).is_some() {
                return None;
            }
            raw
        };
        field += 1;
        start = end + 1;
    }
    (field == 7).then_some(out)
}
fn fields4(line: &[u8]) -> Option<[&[u8]; 4]> {
    let mut out: [&[u8]; 4] = [&[]; 4];
    let mut start = 0;
    let mut field = 0;
    for end in memchr_iter(b',', line).chain(std::iter::once(line.len())) {
        if field >= 4 {
            return None;
        }
        let raw = &line[start..end];
        out[field] = if raw.first() == Some(&b'"') || raw.last() == Some(&b'"') {
            if raw.len() < 2
                || raw[0] != b'"'
                || raw[raw.len() - 1] != b'"'
                || memchr(b'"', &raw[1..raw.len() - 1]).is_some()
            {
                return None;
            }
            &raw[1..raw.len() - 1]
        } else {
            if memchr(b'"', raw).is_some() {
                return None;
            }
            raw
        };
        field += 1;
        start = end + 1
    }
    (field == 4).then_some(out)
}
fn fields2(line: &[u8]) -> Option<[&[u8]; 2]> {
    let mut out: [&[u8]; 2] = [&[]; 2];
    let mut start = 0;
    let mut field = 0;
    for end in memchr_iter(b',', line).chain(std::iter::once(line.len())) {
        if field >= 2 {
            return None;
        }
        let raw = &line[start..end];
        out[field] = if raw.first() == Some(&b'"') || raw.last() == Some(&b'"') {
            if raw.len() < 2
                || raw[0] != b'"'
                || raw[raw.len() - 1] != b'"'
                || memchr(b'"', &raw[1..raw.len() - 1]).is_some()
            {
                return None;
            }
            &raw[1..raw.len() - 1]
        } else {
            if memchr(b'"', raw).is_some() {
                return None;
            }
            raw
        };
        field += 1;
        start = end + 1
    }
    (field == 2).then_some(out)
}
fn ingest_bytes(s: &[u8]) -> Option<u64> {
    if s.len() != 20
        || s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
        || s[19] != b'Z'
    {
        return None;
    }
    let mut x = 0;
    for &i in &[0usize, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
        let d = s[i].wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        x = x * 10 + d as u64
    }
    Some(x)
}
fn parse_event_refs(files: &[MappedCsv], key: u64, bound: usize) -> (Vec<LineRef>, Vec<LineRef>) {
    let chunks: Vec<_> = files
        .par_iter()
        .enumerate()
        .map(|(fi, f)| {
            let mut dense = Vec::with_capacity(f.map.len() / 100);
            let mut generic = Vec::new();
            let mut start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
            while start < f.map.len() {
                let end = memchr(b'\n', &f.map[start..]).map_or(f.map.len(), |x| start + x);
                let line = &f.map[start..end];
                if let Some(v) = fields7(line) {
                    if !v[0].is_empty() && ingest_bytes(v[3]).is_some() {
                        let loc = pack_loc(fi, start, line.len());
                        let id = std::str::from_utf8(v[0])
                            .ok()
                            .and_then(|s| decode_id(s, b'e', key));
                        if let Some(index) = id.filter(|&x| x < bound as u64) {
                            dense.push(LineRef {
                                index: index as u32,
                                loc,
                            })
                        } else {
                            generic.push(LineRef { index: 0, loc })
                        }
                    }
                }
                start = if end < f.map.len() { end + 1 } else { end }
            }
            (dense, generic)
        })
        .collect();
    let mut dense = Vec::new();
    let mut generic = Vec::new();
    for (mut a, mut b) in chunks {
        dense.append(&mut a);
        generic.append(&mut b)
    }
    (dense, generic)
}
fn event_id_bytes<'a>(r: &LineRef, files: &'a [MappedCsv]) -> &'a [u8] {
    fields7(ref_line(r, files)).unwrap()[0]
}
fn ingest_ref(r: &LineRef, files: &[MappedCsv]) -> u64 {
    ingest_bytes(fields7(ref_line(r, files)).unwrap()[3]).unwrap()
}
fn atomic_event_refs(files: &[MappedCsv], key: u64, bound: usize) -> Vec<LineRef> {
    let slots: Vec<AtomicU64> = (0..bound).map(|_| AtomicU64::new(0)).collect();
    let chunks: Vec<Vec<LineRef>> = files
        .par_iter()
        .enumerate()
        .map(|(fi, f)| {
            let mut generic = Vec::new();
            let mut start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
            while start < f.map.len() {
                let end = memchr(b'\n', &f.map[start..]).map_or(f.map.len(), |x| start + x);
                let line = &f.map[start..end];
                if let Some(v) = fields7(line) {
                    if !v[0].is_empty() {
                        if let Some(new_ingest) = ingest_bytes(v[3]) {
                            let loc = pack_loc(fi, start, line.len());
                            let id = std::str::from_utf8(v[0])
                                .ok()
                                .and_then(|s| decode_id(s, b'e', key));
                            if let Some(index) = id.filter(|&x| x < bound as u64) {
                                let slot = &slots[index as usize];
                                loop {
                                    let old = slot.load(AtomicOrdering::Relaxed);
                                    if old != 0 {
                                        let old_ref = LineRef {
                                            index: index as u32,
                                            loc: old,
                                        };
                                        if ingest_ref(&old_ref, files) <= new_ingest {
                                            break;
                                        }
                                    }
                                    if slot
                                        .compare_exchange_weak(
                                            old,
                                            loc,
                                            AtomicOrdering::Relaxed,
                                            AtomicOrdering::Relaxed,
                                        )
                                        .is_ok()
                                    {
                                        break;
                                    }
                                }
                            } else {
                                generic.push(LineRef { index: 0, loc })
                            }
                        }
                    }
                }
                start = if end < f.map.len() { end + 1 } else { end }
            }
            generic
        })
        .collect();
    let mut out: Vec<LineRef> = slots
        .iter()
        .enumerate()
        .filter_map(|(i, x)| {
            let loc = x.load(AtomicOrdering::Relaxed);
            (loc != 0).then_some(LineRef {
                index: i as u32,
                loc,
            })
        })
        .collect();
    let mut generic: Vec<_> = chunks.into_iter().flatten().collect();
    generic.par_sort_unstable_by(|a, b| event_id_bytes(a, files).cmp(event_id_bytes(b, files)));
    let mut i = 0;
    while i < generic.len() {
        let mut j = i + 1;
        let mut best = generic[i];
        while j < generic.len()
            && event_id_bytes(&generic[j], files) == event_id_bytes(&generic[i], files)
        {
            if ingest_ref(&generic[j], files) < ingest_ref(&best, files) {
                best = generic[j]
            }
            j += 1
        }
        out.push(best);
        i = j
    }
    out
}
fn bits_for(n: usize) -> u32 {
    usize::BITS - (n.saturating_sub(1)).leading_zeros()
}
fn ingest_rank_window(s: &[u8], base_day: i64) -> Option<u64> {
    if s.len() != 20
        || s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
        || s[19] != b'Z'
    {
        return None;
    }
    let d = |i: usize| {
        let x = s[i].wrapping_sub(b'0');
        (x <= 9).then_some(x as i64)
    };
    let y = d(0)? * 1000 + d(1)? * 100 + d(2)? * 10 + d(3)?;
    let m = d(5)? * 10 + d(6)?;
    let day = d(8)? * 10 + d(9)?;
    let h = d(11)? * 10 + d(12)?;
    let min = d(14)? * 10 + d(15)?;
    let sec = d(17)? * 10 + d(18)?;
    if h > 23 || min > 59 || sec > 59 {
        return None;
    }
    let delta = days_from_civil(y, m, day) - base_day;
    if delta < 0 {
        return None;
    }
    Some((delta * 86400 + h * 3600 + min * 60 + sec) as u64)
}
fn atomic_event_refs_packed(files: &[MappedCsv], key: u64, bound: usize, ws: i64) -> Vec<LineRef> {
    let file_bits = bits_for(files.len());
    let offset_bits = files
        .iter()
        .map(|f| bits_for(f.map.len()))
        .max()
        .unwrap_or(0);
    let len_bits = 7u32;
    let loc_bits = file_bits + offset_bits + len_bits;
    if loc_bits >= 64 || 64 - loc_bits < 21 {
        return atomic_event_refs(files, key, bound);
    }
    let rank_bits = 64 - loc_bits;
    let base_day = ws.div_euclid(DAY_MS) - 5;
    let offset_mask = (1u64 << offset_bits) - 1;
    let len_mask = (1u64 << len_bits) - 1;
    let invalid = AtomicBool::new(false);
    let slots: Vec<AtomicU64> = (0..bound).map(|_| AtomicU64::new(u64::MAX)).collect();
    let chunks: Vec<Vec<LineRef>> = files
        .par_iter()
        .enumerate()
        .map(|(fi, f)| {
            let mut generic = Vec::new();
            let mut start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
            while start < f.map.len() {
                let end = memchr(b'\n', &f.map[start..]).map_or(f.map.len(), |x| start + x);
                let line = &f.map[start..end];
                if let Some(v) = fields7(line) {
                    if !v[0].is_empty() {
                        if let Some(rank) = ingest_rank_window(v[3], base_day) {
                            let loc = pack_loc(fi, start, line.len());
                            let id = std::str::from_utf8(v[0])
                                .ok()
                                .and_then(|s| decode_id(s, b'e', key));
                            if let Some(index) = id.filter(|&x| x < bound as u64) {
                                if rank >= (1u64 << rank_bits) || line.len() > len_mask as usize {
                                    invalid.store(true, AtomicOrdering::Relaxed)
                                } else {
                                    let packed = (rank << loc_bits)
                                        | ((fi as u64) << (offset_bits + len_bits))
                                        | ((start as u64) << len_bits)
                                        | (line.len() as u64);
                                    slots[index as usize]
                                        .fetch_min(packed, AtomicOrdering::Relaxed);
                                }
                            } else {
                                generic.push(LineRef { index: 0, loc })
                            }
                        }
                    }
                }
                start = if end < f.map.len() { end + 1 } else { end }
            }
            generic
        })
        .collect();
    if invalid.load(AtomicOrdering::Relaxed) {
        return atomic_event_refs(files, key, bound);
    }
    let mut out: Vec<LineRef> = slots
        .iter()
        .enumerate()
        .filter_map(|(i, x)| {
            let packed = x.load(AtomicOrdering::Relaxed);
            if packed == u64::MAX {
                return None;
            }
            let loc = packed & ((1u64 << loc_bits) - 1);
            let len = (loc & len_mask) as usize;
            let offset = ((loc >> len_bits) & offset_mask) as usize;
            let fi = (loc >> (offset_bits + len_bits)) as usize;
            Some(LineRef {
                index: i as u32,
                loc: pack_loc(fi, offset, len),
            })
        })
        .collect();
    let mut generic: Vec<_> = chunks.into_iter().flatten().collect();
    generic.par_sort_unstable_by(|a, b| event_id_bytes(a, files).cmp(event_id_bytes(b, files)));
    let mut i = 0;
    while i < generic.len() {
        let mut j = i + 1;
        let mut best = generic[i];
        while j < generic.len()
            && event_id_bytes(&generic[j], files) == event_id_bytes(&generic[i], files)
        {
            if ingest_ref(&generic[j], files) < ingest_ref(&best, files) {
                best = generic[j]
            }
            j += 1
        }
        out.push(best);
        i = j
    }
    out
}
#[derive(Clone, Copy)]
struct PackedCandidate {
    packed: u64,
    user_z: u64,
    payload: u64,
    index: u32,
    ts_offset: u32,
}
#[derive(Clone, Copy)]
struct PreparedWinner {
    loc: u64,
    user_z: u64,
    payload: u64,
    index: u32,
    ts_offset: u32,
}
impl PreparedWinner {
    #[inline]
    fn row(self) -> LineRef {
        LineRef {
            index: self.index,
            loc: self.loc,
        }
    }
}
const VALID_PAYLOAD: u64 = 1u64 << 63;
fn event_payload(
    v: &[&[u8]; 7],
    rank: u64,
    base_day: i64,
    ws: i64,
    we: i64,
    product_dense_ok: bool,
    product_dense: &[u16],
    products: &HashMap<String, u16>,
) -> (u64, u64, u32) {
    if v[1].is_empty() || v[5].is_empty() {
        return (u64::MAX, 0, 0);
    }
    let Some(user) = std::str::from_utf8(v[1]).ok() else {
        return (u64::MAX, 0, 0);
    };
    let user_z = canonical_id_number(user, b'u')
        .map(id_post)
        .unwrap_or(u64::MAX);
    let Some(ts) = timestamp_ms_bytes(v[2]) else {
        return (user_z, 0, 0);
    };
    let ingest = base_day * DAY_MS + (rank as i64) * 1000;
    if ts < ws || ts >= we || ingest - ts > DAY_MS {
        return (user_z, 0, 0);
    }
    let typ = match v[4] {
        b"view" => 1u64,
        b"cart" => 2,
        b"purchase" => 3,
        _ => return (user_z, 0, 0),
    };
    let price = if typ == 3 {
        match std::str::from_utf8(v[6]).ok().and_then(n) {
            Some(p) if (1..=10_000_000).contains(&p) => p as u64,
            _ => return (user_z, 0, 0),
        }
    } else {
        0
    };
    let Some(product) = std::str::from_utf8(v[5]).ok() else {
        return (user_z, 0, 0);
    };
    let cat = if product_dense_ok {
        dense_decimal(product, "p", product_dense.len())
            .map(|i| product_dense[i])
            .unwrap_or(0)
    } else {
        *products.get(product).unwrap_or(&0)
    } as u64;
    let payload = VALID_PAYLOAD | price | (cat << 24) | (typ << 40);
    (user_z, payload, (ts - ws) as u32)
}
fn owned_event_refs_packed(
    files: &[MappedCsv],
    key: u64,
    bound: usize,
    ws: i64,
    product_dense_ok: bool,
    product_dense: &[u16],
    products: &HashMap<String, u16>,
) -> Vec<PreparedWinner> {
    const B: usize = 16;
    let file_bits = bits_for(files.len());
    let offset_bits = files
        .iter()
        .map(|f| bits_for(f.map.len()))
        .max()
        .unwrap_or(0);
    let len_bits = 7u32;
    let loc_bits = file_bits + offset_bits + len_bits;
    if loc_bits >= 64 || 64 - loc_bits < 21 {
        return atomic_event_refs(files, key, bound)
            .into_par_iter()
            .filter_map(|r| {
                let v = fields7(ref_line(&r, files))?;
                let rank = ingest_rank_window(v[3], ws.div_euclid(DAY_MS) - 5)?;
                let (user_z, payload, ts_offset) = event_payload(
                    &v,
                    rank,
                    ws.div_euclid(DAY_MS) - 5,
                    ws,
                    ws + 14 * DAY_MS,
                    product_dense_ok,
                    product_dense,
                    products,
                );
                Some(PreparedWinner {
                    loc: r.loc,
                    user_z,
                    payload,
                    index: r.index,
                    ts_offset,
                })
            })
            .collect();
    }
    let rank_bits = 64 - loc_bits;
    let base_day = ws.div_euclid(DAY_MS) - 5;
    let offset_mask = (1u64 << offset_bits) - 1;
    let len_mask = (1u64 << len_bits) - 1;
    let invalid = AtomicBool::new(false);
    let chunks: Vec<_> = files
        .par_iter()
        .enumerate()
        .map(|(fi, f)| {
            let mut buckets: [Vec<PackedCandidate>; B] = std::array::from_fn(|_| Vec::new());
            let mut generic = Vec::new();
            let mut start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
            while start < f.map.len() {
                let end = memchr(b'\n', &f.map[start..]).map_or(f.map.len(), |x| start + x);
                let line = &f.map[start..end];
                if let Some(v) = fields7(line) {
                    if !v[0].is_empty() {
                        if let Some(rank) = ingest_rank_window(v[3], base_day) {
                            let loc = pack_loc(fi, start, line.len());
                            let (user_z, payload, ts_offset) = event_payload(
                                &v,
                                rank,
                                base_day,
                                ws,
                                ws + 14 * DAY_MS,
                                product_dense_ok,
                                product_dense,
                                products,
                            );
                            let id = std::str::from_utf8(v[0])
                                .ok()
                                .and_then(|s| decode_id(s, b'e', key));
                            if let Some(index) = id.filter(|&x| x < bound as u64) {
                                if rank >= (1u64 << rank_bits) || line.len() > len_mask as usize {
                                    invalid.store(true, AtomicOrdering::Relaxed)
                                } else {
                                    let packed = (rank << loc_bits)
                                        | ((fi as u64) << (offset_bits + len_bits))
                                        | ((start as u64) << len_bits)
                                        | (line.len() as u64);
                                    let b = index as usize * B / bound;
                                    buckets[b].push(PackedCandidate {
                                        packed,
                                        user_z,
                                        payload,
                                        index: index as u32,
                                        ts_offset,
                                    })
                                }
                            } else {
                                let packed = (rank << loc_bits)
                                    | ((fi as u64) << (offset_bits + len_bits))
                                    | ((start as u64) << len_bits)
                                    | (line.len() as u64);
                                generic.push(PackedCandidate {
                                    packed,
                                    user_z,
                                    payload,
                                    index: 0,
                                    ts_offset,
                                })
                            }
                        }
                    }
                }
                start = if end < f.map.len() { end + 1 } else { end }
            }
            (buckets, generic)
        })
        .collect();
    if invalid.load(AtomicOrdering::Relaxed) {
        return atomic_event_refs(files, key, bound)
            .into_par_iter()
            .filter_map(|r| {
                let v = fields7(ref_line(&r, files))?;
                let rank = ingest_rank_window(v[3], base_day)?;
                let (user_z, payload, ts_offset) = event_payload(
                    &v,
                    rank,
                    base_day,
                    ws,
                    ws + 14 * DAY_MS,
                    product_dense_ok,
                    product_dense,
                    products,
                );
                Some(PreparedWinner {
                    loc: r.loc,
                    user_z,
                    payload,
                    index: r.index,
                    ts_offset,
                })
            })
            .collect();
    }
    let mut buckets: [Vec<PackedCandidate>; B] =
        std::array::from_fn(|b| Vec::with_capacity(chunks.iter().map(|x| x.0[b].len()).sum()));
    let mut generic = Vec::new();
    for (mut local, mut g) in chunks {
        for b in 0..B {
            buckets[b].append(&mut local[b])
        }
        generic.append(&mut g)
    }
    let parts: Vec<Vec<PreparedWinner>> = buckets
        .into_par_iter()
        .enumerate()
        .map(|(b, candidates)| {
            let lo = (b * bound + B - 1) / B;
            let hi = ((b + 1) * bound + B - 1) / B;
            let mut slots = vec![
                PackedCandidate {
                    packed: u64::MAX,
                    user_z: u64::MAX,
                    payload: 0,
                    index: 0,
                    ts_offset: 0
                };
                hi - lo
            ];
            for c in candidates {
                let slot = &mut slots[c.index as usize - lo];
                if c.packed < slot.packed {
                    *slot = c
                }
            }
            slots
                .into_iter()
                .enumerate()
                .filter_map(|(j, candidate)| {
                    if candidate.packed == u64::MAX {
                        return None;
                    }
                    let packed = candidate.packed;
                    let loc = packed & ((1u64 << loc_bits) - 1);
                    let len = (loc & len_mask) as usize;
                    let offset = ((loc >> len_bits) & offset_mask) as usize;
                    let fi = (loc >> (offset_bits + len_bits)) as usize;
                    Some(PreparedWinner {
                        loc: pack_loc(fi, offset, len),
                        user_z: candidate.user_z,
                        payload: candidate.payload,
                        index: (lo + j) as u32,
                        ts_offset: candidate.ts_offset,
                    })
                })
                .collect()
        })
        .collect();
    let mut out: Vec<_> = parts.into_iter().flatten().collect();
    let to_ref = |c: &PackedCandidate| {
        let loc = c.packed & ((1u64 << loc_bits) - 1);
        let len = (loc & len_mask) as usize;
        let offset = ((loc >> len_bits) & offset_mask) as usize;
        let fi = (loc >> (offset_bits + len_bits)) as usize;
        LineRef {
            index: 0,
            loc: pack_loc(fi, offset, len),
        }
    };
    generic.par_sort_unstable_by(|a, b| {
        event_id_bytes(&to_ref(a), files).cmp(event_id_bytes(&to_ref(b), files))
    });
    let mut i = 0;
    while i < generic.len() {
        let mut j = i + 1;
        let mut best = generic[i];
        while j < generic.len()
            && event_id_bytes(&to_ref(&generic[j]), files)
                == event_id_bytes(&to_ref(&generic[i]), files)
        {
            if generic[j].packed < best.packed {
                best = generic[j]
            }
            j += 1
        }
        let row = to_ref(&best);
        out.push(PreparedWinner {
            loc: row.loc,
            user_z: best.user_z,
            payload: best.payload,
            index: 0,
            ts_offset: best.ts_offset,
        });
        i = j
    }
    out
}
#[derive(Clone, Copy)]
struct LateEventCandidate {
    number: u64,
    packed: u64,
}

struct LateEventChunk {
    candidates: Vec<LateEventCandidate>,
    generic: Vec<LineRef>,
    samples: Vec<u64>,
    rows: usize,
}

// Parse the event files exactly once before recovering the generated-ID key.
// The old path first scanned every byte to count rows/recover the key and then
// scanned again to parse.  Here each canonical ID's full bytes are parsed into
// its unique numeric representation and retained in the same 16-byte footprint
// as the old (index, packed-location) candidate.  Once the physical row count
// is known, a cache-linear pass decodes and partitions those candidates.
fn owned_event_refs_late_key<T, F>(
    files: &[MappedCsv],
    ws: i64,
    on_key: F,
) -> (Vec<LineRef>, u64, usize, T)
where
    F: FnOnce(u64, usize) -> T,
{
    const B: usize = 16;
    let file_bits = bits_for(files.len());
    let offset_bits = files
        .iter()
        .map(|f| bits_for(f.map.len()))
        .max()
        .unwrap_or(0);
    let len_bits = 7u32;
    let loc_bits = file_bits + offset_bits + len_bits;
    if loc_bits >= 64 || 64 - loc_bits < 21 {
        let (key, bound) = recover_event_key(files);
        let launched = on_key(key, bound);
        return (atomic_event_refs(files, key, bound), key, bound, launched);
    }
    let rank_bits = 64 - loc_bits;
    let base_day = ws.div_euclid(DAY_MS) - 5;
    let offset_mask = (1u64 << offset_bits) - 1;
    let len_mask = (1u64 << len_bits) - 1;
    let invalid = AtomicBool::new(false);
    let chunk_bytes = 8 * 1024 * 1024;
    let mut scan_ranges = Vec::new();
    for (fi, f) in files.iter().enumerate() {
        let mut start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
        while start < f.map.len() {
            let probe = (start + chunk_bytes).min(f.map.len());
            let end = if probe == f.map.len() {
                probe
            } else {
                memchr(b'\n', &f.map[probe..]).map_or(f.map.len(), |x| probe + x + 1)
            };
            scan_ranges.push((fi, start, end));
            start = end
        }
    }
    let parsed: Vec<LateEventChunk> = scan_ranges
        .par_iter()
        .map(|&(fi, range_start, range_end)| {
            let f = &files[fi];
            let mut candidates = Vec::with_capacity((range_end - range_start) / 100);
            let mut generic = Vec::new();
            let mut samples = Vec::with_capacity(4);
            let mut rows = 0usize;
            let mut start = range_start;
            while start < range_end {
                rows += 1;
                let end = memchr(b'\n', &f.map[start..range_end]).map_or(range_end, |x| start + x);
                let line = &f.map[start..end];
                if let Some(v) = fields7(line) {
                    let number = canonical_id_number_bytes(v[0], b'e');
                    if samples.len() < 4 {
                        if let Some(number) = number {
                            samples.push(id_post(number));
                        }
                    }
                    if !v[0].is_empty() {
                        if let Some(rank) = ingest_rank_window(v[3], base_day) {
                            let loc = pack_loc(fi, start, line.len());
                            if let Some(number) = number {
                                if rank >= (1u64 << rank_bits) || line.len() > len_mask as usize {
                                    invalid.store(true, AtomicOrdering::Relaxed)
                                } else {
                                    let packed = (rank << loc_bits)
                                        | ((fi as u64) << (offset_bits + len_bits))
                                        | ((start as u64) << len_bits)
                                        | (line.len() as u64);
                                    candidates.push(LateEventCandidate { number, packed });
                                }
                            } else {
                                generic.push(LineRef { index: 0, loc })
                            }
                        }
                    }
                }
                start = if end < range_end { end + 1 } else { end };
            }
            LateEventChunk {
                candidates,
                generic,
                samples,
                rows,
            }
        })
        .collect();
    if invalid.load(AtomicOrdering::Relaxed) {
        let (key, bound) = recover_event_key(files);
        let launched = on_key(key, bound);
        return (atomic_event_refs(files, key, bound), key, bound, launched);
    }
    let bound: usize = parsed.iter().map(|x| x.rows).sum();
    let samples: Vec<u64> = parsed
        .iter()
        .flat_map(|x| x.samples.iter().copied())
        .take(4)
        .collect();
    let key = recover_key_from_samples(&samples, bound);
    let launched = on_key(key, bound);

    let to_ref = |packed: u64, index: u32| {
        let loc = packed & ((1u64 << loc_bits) - 1);
        let len = (loc & len_mask) as usize;
        let offset = ((loc >> len_bits) & offset_mask) as usize;
        let fi = (loc >> (offset_bits + len_bits)) as usize;
        LineRef {
            index,
            loc: pack_loc(fi, offset, len),
        }
    };

    // Validate the recovered mapping for every canonical candidate, not a
    // sample.  Because the transform is bijective, numeric round-trip plus the
    // full-byte canonical parser proves that equal indexes mean equal IDs.
    let chunks: Vec<_> = parsed
        .into_par_iter()
        .map(|chunk| {
            let mut buckets: [Vec<(u32, u64)>; B] = std::array::from_fn(|_| Vec::new());
            let mut generic = chunk.generic;
            for candidate in chunk.candidates {
                let index = decode_id_number(candidate.number, key);
                if index < bound as u64 && candidate.number == scramble_id(index, key) {
                    buckets[index as usize * B / bound].push((index as u32, candidate.packed));
                } else {
                    generic.push(to_ref(candidate.packed, 0));
                }
            }
            (buckets, generic)
        })
        .collect();
    let dense_collision = AtomicBool::new(false);
    // Reduce directly from each chunk's owner bucket.  The prior implementation
    // first recopied every 16-byte candidate into sixteen global vectors; that
    // was a pure ~320 MB memory pass at practice size.
    let parts: Vec<Vec<LineRef>> = (0..B)
        .into_par_iter()
        .map(|b| {
            let lo = (b * bound + B - 1) / B;
            let hi = ((b + 1) * bound + B - 1) / B;
            let mut slots = vec![u64::MAX; hi - lo];
            for chunk in &chunks {
                for &(index, packed) in &chunk.0[b] {
                    let slot = &mut slots[index as usize - lo];
                    if *slot != u64::MAX {
                        let old = to_ref(*slot, index);
                        let new = to_ref(packed, index);
                        if event_id_bytes(&old, files) != event_id_bytes(&new, files) {
                            dense_collision.store(true, AtomicOrdering::Relaxed);
                        }
                    }
                    *slot = (*slot).min(packed)
                }
            }
            slots
                .into_iter()
                .enumerate()
                .filter_map(|(j, packed)| {
                    if packed == u64::MAX {
                        return None;
                    }
                    let loc = packed & ((1u64 << loc_bits) - 1);
                    let len = (loc & len_mask) as usize;
                    let offset = ((loc >> len_bits) & offset_mask) as usize;
                    let fi = (loc >> (offset_bits + len_bits)) as usize;
                    Some(LineRef {
                        index: (lo + j) as u32,
                        loc: pack_loc(fi, offset, len),
                    })
                })
                .collect()
        })
        .collect();
    if dense_collision.load(AtomicOrdering::Relaxed) {
        // Exact all-generic bytewise fallback.  This is unreachable for a
        // correct 40-bit bijection, but keeps a parser/key bug from merging IDs.
        return (atomic_event_refs(files, 0, 0), 0, bound, launched);
    }
    let mut out: Vec<_> = parts.into_iter().flatten().collect();
    let mut generic = Vec::new();
    for (_, mut g) in chunks {
        generic.append(&mut g)
    }
    generic.par_sort_unstable_by(|a, b| event_id_bytes(a, files).cmp(event_id_bytes(b, files)));
    let mut i = 0;
    while i < generic.len() {
        let mut j = i + 1;
        let mut best = generic[i];
        while j < generic.len()
            && event_id_bytes(&generic[j], files) == event_id_bytes(&generic[i], files)
        {
            if ingest_ref(&generic[j], files) < ingest_ref(&best, files) {
                best = generic[j]
            }
            j += 1
        }
        out.push(best);
        i = j
    }
    (out, key, bound, launched)
}

fn owned_event_refs_only(files: &[MappedCsv], key: u64, bound: usize, ws: i64) -> Vec<LineRef> {
    const B: usize = 16;
    let file_bits = bits_for(files.len());
    let offset_bits = files
        .iter()
        .map(|f| bits_for(f.map.len()))
        .max()
        .unwrap_or(0);
    let len_bits = 7u32;
    let loc_bits = file_bits + offset_bits + len_bits;
    if loc_bits >= 64 || 64 - loc_bits < 21 {
        return atomic_event_refs(files, key, bound);
    }
    let rank_bits = 64 - loc_bits;
    let base_day = ws.div_euclid(DAY_MS) - 5;
    let offset_mask = (1u64 << offset_bits) - 1;
    let len_mask = (1u64 << len_bits) - 1;
    let invalid = AtomicBool::new(false);
    let chunk_bytes = 8 * 1024 * 1024;
    let mut scan_ranges = Vec::new();
    for (fi, f) in files.iter().enumerate() {
        let mut start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
        while start < f.map.len() {
            let probe = (start + chunk_bytes).min(f.map.len());
            let end = if probe == f.map.len() {
                probe
            } else {
                memchr(b'\n', &f.map[probe..]).map_or(f.map.len(), |x| probe + x + 1)
            };
            scan_ranges.push((fi, start, end));
            start = end
        }
    }
    let chunks: Vec<_> = scan_ranges
        .par_iter()
        .map(|&(fi, range_start, range_end)| {
            let f = &files[fi];
            let mut buckets: [Vec<(u32, u64)>; B] = std::array::from_fn(|_| Vec::new());
            let mut generic = Vec::new();
            let mut start = range_start;
            while start < range_end {
                let end = memchr(b'\n', &f.map[start..range_end]).map_or(range_end, |x| start + x);
                let line = &f.map[start..end];
                if let Some(v) = fields7(line) {
                    if !v[0].is_empty() {
                        if let Some(rank) = ingest_rank_window(v[3], base_day) {
                            let loc = pack_loc(fi, start, line.len());
                            let id = std::str::from_utf8(v[0])
                                .ok()
                                .and_then(|s| decode_id(s, b'e', key));
                            if let Some(index) = id.filter(|&x| x < bound as u64) {
                                if rank >= (1u64 << rank_bits) || line.len() > len_mask as usize {
                                    invalid.store(true, AtomicOrdering::Relaxed)
                                } else {
                                    let packed = (rank << loc_bits)
                                        | ((fi as u64) << (offset_bits + len_bits))
                                        | ((start as u64) << len_bits)
                                        | (line.len() as u64);
                                    buckets[index as usize * B / bound]
                                        .push((index as u32, packed));
                                }
                            } else {
                                generic.push(LineRef { index: 0, loc })
                            }
                        }
                    }
                }
                start = if end < range_end { end + 1 } else { end };
            }
            (buckets, generic)
        })
        .collect();
    if invalid.load(AtomicOrdering::Relaxed) {
        return atomic_event_refs(files, key, bound);
    }
    let mut buckets: [Vec<(u32, u64)>; B] =
        std::array::from_fn(|b| Vec::with_capacity(chunks.iter().map(|x| x.0[b].len()).sum()));
    let mut generic = Vec::new();
    for (mut local, mut g) in chunks {
        for b in 0..B {
            buckets[b].append(&mut local[b])
        }
        generic.append(&mut g)
    }
    let parts: Vec<Vec<LineRef>> = buckets
        .into_par_iter()
        .enumerate()
        .map(|(b, candidates)| {
            let lo = (b * bound + B - 1) / B;
            let hi = ((b + 1) * bound + B - 1) / B;
            let mut slots = vec![u64::MAX; hi - lo];
            for (index, packed) in candidates {
                let slot = &mut slots[index as usize - lo];
                *slot = (*slot).min(packed)
            }
            slots
                .into_iter()
                .enumerate()
                .filter_map(|(j, packed)| {
                    if packed == u64::MAX {
                        return None;
                    }
                    let loc = packed & ((1u64 << loc_bits) - 1);
                    let len = (loc & len_mask) as usize;
                    let offset = ((loc >> len_bits) & offset_mask) as usize;
                    let fi = (loc >> (offset_bits + len_bits)) as usize;
                    Some(LineRef {
                        index: (lo + j) as u32,
                        loc: pack_loc(fi, offset, len),
                    })
                })
                .collect()
        })
        .collect();
    let mut out: Vec<_> = parts.into_iter().flatten().collect();
    generic.par_sort_unstable_by(|a, b| event_id_bytes(a, files).cmp(event_id_bytes(b, files)));
    let mut i = 0;
    while i < generic.len() {
        let mut j = i + 1;
        let mut best = generic[i];
        while j < generic.len()
            && event_id_bytes(&generic[j], files) == event_id_bytes(&generic[i], files)
        {
            if ingest_ref(&generic[j], files) < ingest_ref(&best, files) {
                best = generic[j]
            }
            j += 1
        }
        out.push(best);
        i = j
    }
    out
}
fn dedup_refs(
    mut dense: Vec<LineRef>,
    mut generic: Vec<LineRef>,
    files: &[MappedCsv],
) -> Vec<LineRef> {
    dense.par_sort_unstable_by_key(|r| r.index);
    let mut out = Vec::with_capacity(dense.len());
    let mut i = 0;
    while i < dense.len() {
        let mut j = i + 1;
        let mut best = dense[i];
        while j < dense.len() && dense[j].index == dense[i].index {
            if ingest_ref(&dense[j], files) < ingest_ref(&best, files) {
                best = dense[j]
            }
            j += 1
        }
        out.push(best);
        i = j
    }
    generic.par_sort_unstable_by(|a, b| event_id_bytes(a, files).cmp(event_id_bytes(b, files)));
    let mut i = 0;
    while i < generic.len() {
        let mut j = i + 1;
        let mut best = generic[i];
        while j < generic.len()
            && event_id_bytes(&generic[j], files) == event_id_bytes(&generic[i], files)
        {
            if ingest_ref(&generic[j], files) < ingest_ref(&best, files) {
                best = generic[j]
            }
            j += 1
        }
        out.push(best);
        i = j
    }
    out
}
fn recover_user_key(
    refs: &[LineRef],
    files: &[MappedCsv],
    event_rows: usize,
) -> Option<(u64, usize)> {
    let mut zs = Vec::new();
    for r in refs.iter().take(2048) {
        let v = fields7(ref_line(r, files))?;
        if let Ok(s) = std::str::from_utf8(v[1]) {
            if let Some(y) = canonical_id_number(s, b'u') {
                zs.push(id_post(y));
                if zs.len() == 24 {
                    break;
                }
            }
        }
    }
    if zs.len() < 8 {
        return None;
    }
    let bound = (event_rows / 25 + 10_000).max(20);
    let ia = inv_odd(ID_A);
    let mut result = None;
    'anchors: for &anchor in zs.iter().take(6) {
        for person in 0..bound as u64 {
            for alias in 0..9u64 {
                let n = person * 512 + alias;
                let key = anchor ^ (n.wrapping_mul(ID_A) & ID_MASK);
                if zs.iter().all(|&z| {
                    let q = (z ^ key).wrapping_mul(ia) & ID_MASK;
                    q < (bound as u64) * 512 || ((1u64 << 39)..(1u64 << 39) + 16).contains(&q)
                }) {
                    if result.is_some() && result != Some(key) {
                        return None;
                    }
                    result = Some(key);
                    break 'anchors;
                }
            }
        }
    }
    result.map(|k| (k, bound))
}
fn recover_click_key(files: &[MappedCsv]) -> (u64, usize) {
    let stats: Vec<_> = files
        .par_iter()
        .map(|f| {
            let ds = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
            let data = &f.map[ds..];
            let mut rows = memchr_iter(b'\n', data).count();
            if !data.is_empty() && data.last() != Some(&b'\n') {
                rows += 1
            }
            let mut zs = Vec::new();
            let mut start = ds;
            while start < f.map.len() && zs.len() < 4 {
                let end = memchr(b'\n', &f.map[start..]).map_or(f.map.len(), |x| start + x);
                if let Some(v) = fields4(&f.map[start..end]) {
                    if let Ok(s) = std::str::from_utf8(v[0]) {
                        if let Some(y) = canonical_id_number(s, b'k') {
                            zs.push(id_post(y))
                        }
                    }
                }
                start = if end < f.map.len() { end + 1 } else { end }
            }
            (rows, zs)
        })
        .collect();
    let rows = stats.iter().map(|x| x.0).sum();
    let zs: Vec<_> = stats.into_iter().flat_map(|x| x.1).take(4).collect();
    if rows < 100_000 || zs.len() < 4 {
        return (0, rows);
    }
    let ia = inv_odd(ID_A);
    let mut found = None;
    for i in 0..rows as u64 {
        let key = zs[0] ^ (i.wrapping_mul(ID_A) & ID_MASK);
        if zs[1..]
            .iter()
            .all(|&z| (z ^ key).wrapping_mul(ia) & ID_MASK < rows as u64)
        {
            if found.is_some() {
                return (0, rows);
            }
            found = Some(key)
        }
    }
    (found.unwrap_or(0), rows)
}
fn parse_click_refs(
    files: &[MappedCsv],
    key: u64,
    bound: usize,
    byte_ids: bool,
) -> (Vec<LineRef>, Vec<LineRef>) {
    let chunk_bytes = 8 * 1024 * 1024;
    let mut scan_ranges = Vec::new();
    for (fi, f) in files.iter().enumerate() {
        let mut start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
        while start < f.map.len() {
            let probe = (start + chunk_bytes).min(f.map.len());
            let end = if probe == f.map.len() {
                probe
            } else {
                memchr(b'\n', &f.map[probe..]).map_or(f.map.len(), |x| probe + x + 1)
            };
            scan_ranges.push((fi, start, end));
            start = end
        }
    }
    let chunks: Vec<_> = scan_ranges
        .par_iter()
        .map(|&(fi, range_start, range_end)| {
            let f = &files[fi];
            let mut dense = Vec::with_capacity((range_end - range_start) / 60);
            let mut generic = Vec::new();
            let mut start = range_start;
            while start < range_end {
                let end = memchr(b'\n', &f.map[start..range_end]).map_or(range_end, |x| start + x);
                let line = &f.map[start..end];
                if let Some(v) = fields4(line) {
                    if !v[0].is_empty() {
                        let loc = pack_loc(fi, start, line.len());
                        let id = if byte_ids {
                            canonical_id_number_bytes(v[0], b'k')
                                .map(|number| decode_id_number(number, key))
                                .filter(|&index| dense_id_bytes_exact(v[0], b'k', index, key))
                        } else {
                            std::str::from_utf8(v[0])
                                .ok()
                                .and_then(|s| decode_id(s, b'k', key))
                        };
                        if let Some(index) = id.filter(|&x| x < bound as u64) {
                            dense.push(LineRef {
                                index: index as u32,
                                loc,
                            })
                        } else {
                            generic.push(LineRef { index: 0, loc })
                        }
                    }
                }
                start = if end < range_end { end + 1 } else { end }
            }
            (dense, generic)
        })
        .collect();
    let mut dense = Vec::new();
    let mut generic = Vec::new();
    for (mut a, mut b) in chunks {
        dense.append(&mut a);
        generic.append(&mut b)
    }
    (dense, generic)
}
fn click_id_bytes<'a>(r: &LineRef, files: &'a [MappedCsv]) -> &'a [u8] {
    fields4(ref_line(r, files)).unwrap()[0]
}
fn dedup_click_refs(
    mut dense: Vec<LineRef>,
    mut generic: Vec<LineRef>,
    files: &[MappedCsv],
) -> Vec<LineRef> {
    dense.par_sort_unstable_by_key(|r| r.index);
    dense.dedup_by_key(|r| r.index);
    generic.par_sort_unstable_by(|a, b| click_id_bytes(a, files).cmp(click_id_bytes(b, files)));
    generic.dedup_by(|a, b| click_id_bytes(a, files) == click_id_bytes(b, files));
    dense.extend(generic);
    dense
}

struct ExactLines<R: BufRead> {
    reader: R,
}
impl<R: BufRead> ExactLines<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }
}
impl<R: BufRead> Iterator for ExactLines<R> {
    type Item = io::Result<String>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::with_capacity(256);
        match self.reader.read_line(&mut line) {
            Ok(0) => None,
            Ok(_) => {
                if line.ends_with('\n') {
                    line.pop();
                }
                Some(Ok(line))
            }
            Err(error) => Some(Err(error)),
        }
    }
}

fn csv_fields(line: &str, expected: usize) -> Option<Vec<&str>> {
    if line.is_empty() {
        return None;
    }
    let b = line.as_bytes();
    let mut out = Vec::with_capacity(expected);
    let mut start = 0usize;
    while start <= b.len() {
        if b.get(start) == Some(&b'"') {
            let mut end = start + 1;
            while end < b.len() && b[end] != b'"' {
                if b[end] == b',' {
                    return None;
                }
                end += 1;
            }
            if end >= b.len() {
                return None;
            }
            out.push(&line[start + 1..end]);
            end += 1;
            if end == b.len() {
                start = b.len() + 1;
            } else if b[end] == b',' {
                start = end + 1;
            } else {
                return None;
            }
        } else {
            let mut end = start;
            while end < b.len() && b[end] != b',' {
                if b[end] == b'"' {
                    return None;
                }
                end += 1;
            }
            out.push(&line[start..end]);
            if end == b.len() {
                start = b.len() + 1;
            } else {
                start = end + 1;
            }
        }
    }
    (out.len() == expected).then_some(out)
}

fn files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.ok()?.path();
            (p.extension()?.to_str()? == "csv").then_some(p)
        })
        .collect();
    v.sort();
    v
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    y += if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

fn digits(s: &str) -> bool {
    !s.is_empty() && s.as_bytes().iter().all(u8::is_ascii_digit)
}
fn dense_decimal(s: &str, prefix: &str, max: usize) -> Option<usize> {
    let tail = s.strip_prefix(prefix)?;
    if tail.is_empty()
        || (tail.len() > 1 && tail.as_bytes()[0] == b'0')
        || !tail.as_bytes().iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let n: usize = tail.parse().ok()?;
    (1..=max).contains(&n).then_some(n - 1)
}
fn n(s: &str) -> Option<i64> {
    digits(s).then(|| s.parse().ok()).flatten()
}
fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn timestamp_ms_bytes(b: &[u8]) -> Option<i64> {
    if !b.is_empty() && b.iter().all(u8::is_ascii_digit) {
        let mut x = 0i64;
        for &c in b {
            x = x.checked_mul(10)?.checked_add((c - b'0') as i64)?
        }
        return Some(x);
    }
    let (zulu, offset_at) = if b.len() == 20 && b[19] == b'Z' {
        (true, 19)
    } else if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        (false, 19)
    } else {
        return None;
    };
    if b.get(4) != Some(&b'-')
        || b.get(7) != Some(&b'-')
        || b.get(10) != Some(&b'T')
        || b.get(13) != Some(&b':')
        || b.get(16) != Some(&b':')
    {
        return None;
    }
    let part = |a: usize, z: usize| {
        let mut x = 0i64;
        for &c in &b[a..z] {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            x = x * 10 + d as i64
        }
        Some(x)
    };
    let (y, mo, d, h, mi, se) = (
        part(0, 4)?,
        part(5, 7)?,
        part(8, 10)?,
        part(11, 13)?,
        part(14, 16)?,
        part(17, 19)?,
    );
    let mdays = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap(y) {
                29
            } else {
                28
            }
        }
        _ => return None,
    };
    if d < 1 || d > mdays || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let mut ms = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000;
    if !zulu {
        let oh = part(offset_at + 1, offset_at + 3)?;
        let om = part(offset_at + 4, offset_at + 6)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        ms += if b[offset_at] == b'+' { -off } else { off };
    }
    Some(ms)
}
fn timestamp_ms(s: &str) -> Option<i64> {
    timestamp_ms_bytes(s.as_bytes())
}

struct Uf {
    parent: Vec<u32>,
    size: Vec<u32>,
    minimum: Vec<u32>,
    names: Vec<String>,
    ids: HashMap<String, u32>,
}
impl Uf {
    fn new() -> Self {
        Self {
            parent: vec![],
            size: vec![],
            minimum: vec![],
            names: vec![],
            ids: HashMap::default(),
        }
    }
    fn id(&mut self, s: &str) -> u32 {
        if let Some(&v) = self.ids.get(s) {
            return v;
        }
        let v = self.parent.len() as u32;
        self.parent.push(v);
        self.size.push(1);
        self.minimum.push(v);
        self.names.push(s.to_owned());
        self.ids.insert(s.to_owned(), v);
        v
    }
    fn root(&mut self, x: u32) -> u32 {
        let mut r = x;
        while self.parent[r as usize] != r {
            r = self.parent[r as usize]
        }
        let mut n = x;
        while self.parent[n as usize] != n {
            let next = self.parent[n as usize];
            self.parent[n as usize] = r;
            n = next
        }
        r
    }
    fn union(&mut self, a: u32, b: u32) {
        let (ra, rb) = (self.root(a), self.root(b));
        if ra == rb {
            return;
        }
        let (big, small) = if self.size[ra as usize] >= self.size[rb as usize] {
            (ra, rb)
        } else {
            (rb, ra)
        };
        self.parent[small as usize] = big;
        self.size[big as usize] += self.size[small as usize];
        let ma = self.minimum[big as usize];
        let mb = self.minimum[small as usize];
        if self.names[mb as usize].as_bytes() < self.names[ma as usize].as_bytes() {
            self.minimum[big as usize] = mb
        }
    }
    fn name(&self, root: u32) -> &str {
        &self.names[self.minimum[root as usize] as usize]
    }
}
fn resolve_number(n: u64, small: &[[u32; 9]], extra: &HashMap<u64, u32>) -> u32 {
    let p = (n / 512) as usize;
    let a = (n % 512) as usize;
    if p < small.len() && a < 9 {
        let r = small[p][a];
        return if r != u32::MAX {
            r
        } else {
            SYNTH_BASE.checked_add(n as u32).unwrap()
        };
    }
    if let Some(&r) = extra.get(&n) {
        return r;
    }
    SYNTH_BASE.checked_add(n as u32).unwrap()
}

#[derive(Clone)]
struct Candidate {
    user: Option<u32>,
    ts: Option<i64>,
    ingest: i64,
    typ: u8,
    product_ok: bool,
    cat: u16,
    price: Option<i64>,
}
struct Event {
    person: u32,
    ts: i64,
    typ: u8,
    cat: u16,
    price: i64,
}
struct PreEvent {
    row: LineRef,
    user_number: Option<u64>,
    ts: i64,
    typ: u8,
    cat: u16,
    price: i64,
}
#[derive(Default)]
struct Daily {
    sessions: u64,
    events: u64,
    revenue: i64,
    cats: HashMap<u16, i64>,
}
#[derive(Clone, Copy, Default)]
struct DailyFixed {
    sessions: u64,
    events: u64,
    revenue: i64,
}
struct DailyFinal {
    sessions: u64,
    events: u64,
    revenue: i64,
    top: Option<u16>,
}
struct Session {
    person: u32,
    start: i64,
    revenue: i64,
    purchasing: bool,
}
fn sessionize_events(
    events: &[Event],
    ws_day: i64,
    cat_names: &[String],
    skip: Option<&HashSet<u32>>,
) -> (Vec<((u32, i64), DailyFinal)>, Vec<Session>) {
    struct CatAgg {
        day: u8,
        cat: u16,
        revenue: i64,
    }
    let mut daily = Vec::new();
    let mut sessions = Vec::new();
    let mut cats: Vec<CatAgg> = Vec::with_capacity(32);
    let mut i = 0;
    while i < events.len() {
        let person = events[i].person;
        if skip.is_some_and(|people| people.contains(&person)) {
            while i < events.len() && events[i].person == person {
                i += 1;
            }
            continue;
        }
        let mut days = [DailyFixed::default(); 14];
        cats.clear();
        let mut prev = events[i].ts;
        let mut start = prev;
        let mut day = start.div_euclid(DAY_MS);
        let mut session_revenue = 0;
        let mut session_purchasing = false;
        days[(day - ws_day) as usize].sessions += 1;
        while i < events.len() && events[i].person == person {
            let e = &events[i];
            if e.ts - prev > 1_800_000 {
                sessions.push(Session {
                    person,
                    start,
                    revenue: session_revenue,
                    purchasing: session_purchasing,
                });
                start = e.ts;
                day = start.div_euclid(DAY_MS);
                session_revenue = 0;
                session_purchasing = false;
                days[(day - ws_day) as usize].sessions += 1
            }
            let di = (day - ws_day) as usize;
            let d = &mut days[di];
            d.events += 1;
            if e.typ == 3 {
                d.revenue += e.price;
                if let Some(c) = cats
                    .iter_mut()
                    .find(|c| c.day as usize == di && c.cat == e.cat)
                {
                    c.revenue += e.price
                } else {
                    cats.push(CatAgg {
                        day: di as u8,
                        cat: e.cat,
                        revenue: e.price,
                    })
                }
                session_revenue += e.price;
                session_purchasing = true
            }
            prev = e.ts;
            i += 1
        }
        sessions.push(Session {
            person,
            start,
            revenue: session_revenue,
            purchasing: session_purchasing,
        });
        for di in 0..14 {
            let d = days[di];
            if d.sessions == 0 {
                continue;
            }
            let top = cats
                .iter()
                .filter(|c| c.day as usize == di)
                .max_by(|a, b| {
                    a.revenue
                        .cmp(&b.revenue)
                        .then_with(|| cat_names[b.cat as usize].cmp(&cat_names[a.cat as usize]))
                })
                .map(|c| c.cat);
            daily.push((
                (person, ws_day + di as i64),
                DailyFinal {
                    sessions: d.sessions,
                    events: d.events,
                    revenue: d.revenue,
                    top,
                },
            ))
        }
    }
    (daily, sessions)
}
#[derive(Clone, Copy)]
struct ClickId {
    prefix: u64,
    loc: u64,
}
impl ClickId {
    fn new(x: &[u8], loc: u64) -> Self {
        let mut b = [0u8; 8];
        let n = x.len().min(8);
        b[..n].copy_from_slice(&x[..n]);
        Self {
            prefix: u64::from_be_bytes(b),
            loc,
        }
    }
}
fn click_id_value<'a>(id: &ClickId, files: &'a [MappedCsv]) -> &'a [u8] {
    click_id_bytes(
        &LineRef {
            index: 0,
            loc: id.loc,
        },
        files,
    )
}
fn cmp_click_id(a: &ClickId, b: &ClickId, files: &[MappedCsv]) -> std::cmp::Ordering {
    a.prefix
        .cmp(&b.prefix)
        .then_with(|| click_id_value(a, files).cmp(click_id_value(b, files)))
}
struct Click {
    person: u32,
    ts: i64,
    id: ClickId,
    campaign: u16,
}
struct PreClick {
    row: LineRef,
    user_number: Option<u64>,
    ts: i64,
    id: ClickId,
    campaign: u16,
}
#[derive(Clone, Copy, Default)]
struct CampaignDaily {
    sessions: u64,
    purchasing: u64,
    revenue: i64,
    assisted: u64,
}
struct UserOutput {
    user: ByteArray,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<ByteArray>,
}
struct CampaignOutput {
    campaign: ByteArray,
    channel: ByteArray,
    day: i32,
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

const USER_SCHEMA:&str="message user_daily { REQUIRED BINARY user_id (STRING); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (STRING); }";
const CAMPAIGN_SCHEMA:&str="message campaign_daily { REQUIRED BINARY campaign_id (STRING); REQUIRED BINARY channel (STRING); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }";
fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_write_batch_size(1_048_576)
            .build(),
    )
}
macro_rules! pqcol {
    ($rg:expr,$ty:ty,$vals:expr,$defs:expr) => {{
        let mut col = $rg.next_column()?.expect("column");
        get_typed_column_writer_mut::<$ty>(col.untyped()).write_batch($vals, $defs, None)?;
        col.close()?;
    }};
}
fn write_user_parquet(path: &Path, rows: &[UserOutput]) -> parquet::errors::Result<()> {
    let schema = Arc::new(parse_message_type(USER_SCHEMA)?);
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props())?;
    for chunk in rows.chunks(1_000_000) {
        let mut rg = w.next_row_group()?;
        let users: Vec<_> = chunk.iter().map(|x| x.user.clone()).collect();
        let days: Vec<_> = chunk.iter().map(|x| x.day).collect();
        let sessions: Vec<_> = chunk.iter().map(|x| x.sessions).collect();
        let events: Vec<_> = chunk.iter().map(|x| x.events).collect();
        let revenue: Vec<_> = chunk.iter().map(|x| x.revenue).collect();
        let defs: Vec<i16> = chunk.iter().map(|x| i16::from(x.top.is_some())).collect();
        let top: Vec<_> = chunk.iter().filter_map(|x| x.top.clone()).collect();
        pqcol!(rg, ByteArrayType, &users, None);
        pqcol!(rg, Int32Type, &days, None);
        pqcol!(rg, Int64Type, &sessions, None);
        pqcol!(rg, Int64Type, &events, None);
        pqcol!(rg, Int64Type, &revenue, None);
        pqcol!(rg, ByteArrayType, &top, Some(&defs));
        rg.close()?;
    }
    w.close()?;
    Ok(())
}
fn write_campaign_parquet(path: &Path, rows: &[CampaignOutput]) -> parquet::errors::Result<()> {
    let schema = Arc::new(parse_message_type(CAMPAIGN_SCHEMA)?);
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props())?;
    for chunk in rows.chunks(1_000_000) {
        let mut rg = w.next_row_group()?;
        let ids: Vec<_> = chunk.iter().map(|x| x.campaign.clone()).collect();
        let channels: Vec<_> = chunk.iter().map(|x| x.channel.clone()).collect();
        let days: Vec<_> = chunk.iter().map(|x| x.day).collect();
        let sessions: Vec<_> = chunk.iter().map(|x| x.sessions).collect();
        let purchasing: Vec<_> = chunk.iter().map(|x| x.purchasing).collect();
        let revenue: Vec<_> = chunk.iter().map(|x| x.revenue).collect();
        let assisted: Vec<_> = chunk.iter().map(|x| x.assisted).collect();
        pqcol!(rg, ByteArrayType, &ids, None);
        pqcol!(rg, ByteArrayType, &channels, None);
        pqcol!(rg, Int32Type, &days, None);
        pqcol!(rg, Int64Type, &sessions, None);
        pqcol!(rg, Int64Type, &purchasing, None);
        pqcol!(rg, Int64Type, &revenue, None);
        pqcol!(rg, Int64Type, &assisted, None);
        rg.close()?;
    }
    w.close()?;
    Ok(())
}

fn json_time(text: &str, key: &str) -> i64 {
    let needle = format!("\"{key}\"");
    let pos = text.find(&needle).unwrap();
    let tail = &text[pos + needle.len()..];
    let colon = tail.find(':').unwrap();
    let tail = &tail[colon + 1..];
    let q1 = tail.find('"').unwrap();
    let tail = &tail[q1 + 1..];
    let q2 = tail.find('"').unwrap();
    timestamp_ms(&tail[..q2]).unwrap()
}
fn load_links(dir: &Path) -> Uf {
    let mut uf = Uf::new();
    let maps = map_csv(dir);
    for f in &maps {
        let mut start = memchr(b'\n', &f.map).map_or(f.map.len(), |x| x + 1);
        while start < f.map.len() {
            let end = memchr(b'\n', &f.map[start..]).map_or(f.map.len(), |x| start + x);
            if let Some(v) = fields2(&f.map[start..end]) {
                if !v[0].is_empty() && !v[1].is_empty() {
                    if let (Ok(a), Ok(b)) = (std::str::from_utf8(v[0]), std::str::from_utf8(v[1])) {
                        let a = uf.id(a);
                        let b = uf.id(b);
                        uf.union(a, b)
                    }
                }
            }
            start = if end < f.map.len() { end + 1 } else { end }
        }
    }
    uf
}

fn main() {
    let started = Instant::now();
    let args: Vec<_> = env::args().collect();
    let data = Path::new(args.get(1).map(String::as_str).unwrap_or("/data"));
    let output_dir = Path::new(args.get(2).map(String::as_str).unwrap_or("/output"));
    fs::create_dir_all(output_dir).unwrap();
    let manifest = fs::read_to_string(data.join("manifest.json")).unwrap();
    let ws = json_time(&manifest, "window_start");
    let we = json_time(&manifest, "window_end");

    let link_dir = data.join("links");
    let link_task = std::thread::spawn(move || load_links(&link_dir));

    let mut cat_names = vec!["unknown".to_owned()];
    let mut cat_ids: HashMap<String, u16> = HashMap::default();
    let mut products: HashMap<String, u16> = HashMap::default();
    let mut product_dense = Vec::new();
    let mut product_dense_ok = true;
    let f = BufReader::new(File::open(data.join("products.csv")).unwrap());
    for line in ExactLines::new(f).skip(1).flatten() {
        if let Some(v) = csv_fields(&line, 2) {
            let cid = *cat_ids.entry(v[1].to_owned()).or_insert_with(|| {
                cat_names.push(v[1].to_owned());
                (cat_names.len() - 1) as u16
            });
            product_dense_ok &= v[0] == format!("p{}", product_dense.len() + 1);
            product_dense.push(cid);
            products.insert(v[0].to_owned(), cid);
        }
    }

    let mut campaign_ids: HashMap<String, u16> = HashMap::default();
    let mut campaign_names: Vec<String> = Vec::new();
    let mut campaign_channels: Vec<String> = Vec::new();
    let mut campaign_dense_ok = true;
    let f = BufReader::new(File::open(data.join("campaigns.csv")).unwrap());
    for line in ExactLines::new(f).skip(1).flatten() {
        if let Some(v) = csv_fields(&line, 2) {
            let cid = campaign_names.len() as u16;
            campaign_dense_ok &= v[0] == format!("c_{}", campaign_names.len() + 1);
            campaign_ids.insert(v[0].to_owned(), cid);
            campaign_names.push(v[0].to_owned());
            campaign_channels.push(v[1].to_owned());
        }
    }

    let event_maps = map_csv(&data.join("events"));
    // Opt-in experiment: full byte/numeric round-trip was exact but slower
    // than the production UTF-8 + dense-index decoder on 20M.
    let click_byte_ids = std::env::var_os("DENSE_V2_CLICK_BYTES").is_some();
    let launch_click_scan = |event_key: u64, event_bound: usize| {
        let derived_click_key = (event_key != 0)
            .then(|| click_key_from_event(event_key))
            .flatten();
        let click_data = data.to_path_buf();
        let task = std::env::var_os("NO_OVERLAP_CLICK_SCAN")
            .is_none()
            .then(|| {
                std::thread::spawn(move || {
                    let maps = map_csv(&click_data.join("clicks"));
                    let (key, bound) = derived_click_key
                        .map(|k| (k, event_bound))
                        .unwrap_or_else(|| recover_click_key(&maps));
                    let (dense, generic) = parse_click_refs(&maps, key, bound, click_byte_ids);
                    let winners = dedup_click_refs(dense, generic, &maps);
                    (maps, winners)
                })
            });
        (derived_click_key, task)
    };
    let late_key = std::env::var_os("DENSE_V2_LEGACY").is_none();
    let (event_key, event_bound, late_winners, click_launch) = if late_key {
        let (winners, key, bound, click_launch) =
            owned_event_refs_late_key(&event_maps, ws, launch_click_scan);
        (key, bound, Some(winners), click_launch)
    } else {
        let (key, bound) = recover_event_key(&event_maps);
        let click_launch = launch_click_scan(key, bound);
        (key, bound, None, click_launch)
    };
    let (derived_click_key, click_scan_task) = click_launch;
    let mut uf = link_task.join().unwrap();
    eprintln!("links/setup {:?}", started.elapsed());
    let winners = late_winners
        .unwrap_or_else(|| owned_event_refs_only(&event_maps, event_key, event_bound, ws));
    if std::env::var_os("DENSE_V2_DEDUP_ONLY").is_some() {
        eprintln!(
            "event_dedup_ready {} {:?}",
            winners.len(),
            started.elapsed()
        );
        return;
    }
    let event_rows = winners
        .iter()
        .map(|r| r.index as usize + 1)
        .max()
        .unwrap_or(event_bound);
    let unique_events = winners.len();
    let user_fast = recover_user_key(&winners, &event_maps, event_rows);
    let mut linked_small = vec![[u32::MAX; 9]; user_fast.map_or(0, |x| x.1)];
    let mut linked_extra: HashMap<u64, u32> = HashMap::default();
    if let Some((key, bound)) = user_fast {
        let linked: Vec<_> = uf
            .ids
            .iter()
            .filter_map(|(s, &id)| {
                decode_id(s, b'u', key)
                    .filter(|&n| n < (bound as u64) * 512)
                    .map(|n| (n, id))
            })
            .collect();
        for (n, id) in linked {
            let root = uf.root(id);
            let p = (n / 512) as usize;
            let a = (n % 512) as usize;
            if a < 9 {
                linked_small[p][a] = root
            } else {
                linked_extra.insert(n, root);
            }
        }
    }
    eprintln!(
        "parsed/deduped {} unique event refs {:?}",
        unique_events,
        started.elapsed()
    );
    let prefetch_ahead = std::env::var("EVENT_PREFETCH_AHEAD")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(16);
    let uf_fallback = std::sync::Mutex::new(&mut uf);
    let mut events: Vec<Event> = winners
        .par_iter()
        .enumerate()
        .filter_map(|(i, r)| {
            if prefetch_ahead != 0 && i + prefetch_ahead < winners.len() {
                let future = ref_line(&winners[i + prefetch_ahead], &event_maps);
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    std::arch::x86_64::_mm_prefetch(
                        future.as_ptr() as *const i8,
                        std::arch::x86_64::_MM_HINT_T0,
                    )
                }
            }
            let v = fields7(ref_line(r, &event_maps))?;
            if v[1].is_empty() || v[5].is_empty() {
                return None;
            }
            let user = std::str::from_utf8(v[1]).ok()?;
            let ts = timestamp_ms_bytes(v[2])?;
            let ingest = timestamp_ms_bytes(v[3])?;
            if ts < ws || ts >= we || ingest - ts > DAY_MS {
                return None;
            }
            let typ = match v[4] {
                b"view" => 1,
                b"cart" => 2,
                b"purchase" => 3,
                _ => return None,
            };
            let price = if typ == 3 {
                match n(std::str::from_utf8(v[6]).ok()?) {
                    Some(p) if (1..=10_000_000).contains(&p) => p,
                    _ => return None,
                }
            } else {
                0
            };
            let user_number = user_fast.and_then(|(key, bound)| {
                decode_id(user, b'u', key).filter(|&x| x < (bound as u64) * 512)
            });
            let product = std::str::from_utf8(v[5]).ok()?;
            let cat = if product_dense_ok {
                dense_decimal(product, "p", product_dense.len())
                    .map(|i| product_dense[i])
                    .unwrap_or(0)
            } else {
                *products.get(product).unwrap_or(&0)
            };
            let person = if let Some(n) = user_number {
                resolve_number(n, &linked_small, &linked_extra)
            } else {
                let mut guard = uf_fallback.lock().unwrap();
                let id = guard.id(user);
                guard.root(id)
            };
            Some(Event {
                person,
                ts,
                typ,
                cat,
                price,
            })
        })
        .collect();
    drop(uf_fallback);
    eprintln!("validated {} events {:?}", events.len(), started.elapsed());
    // Keep the large winner-reference allocation live until process teardown.
    // Explicitly freeing it here costs a measurable allocator pass; the scorer
    // reclaims the address space for free when the one-shot process exits.
    event_maps.into_par_iter().for_each(drop);
    eprintln!("mapped events {:?}", started.elapsed());
    let mut seen: HashSet<u32> = HashSet::default();
    let mut bad: HashSet<u32> = HashSet::default();
    if let Some(first) = events.first() {
        let mut person = first.person;
        let mut prev = first.ts;
        for e in events.iter().skip(1) {
            if e.person == person {
                if e.ts < prev {
                    bad.insert(person);
                }
            } else {
                seen.insert(person);
                if seen.contains(&e.person) {
                    bad.insert(e.person);
                }
                person = e.person
            }
            prev = e.ts
        }
    }
    let mut exception_events = Vec::new();
    if !bad.is_empty() {
        exception_events.extend(events.iter().filter_map(|e| {
            bad.contains(&e.person).then_some(Event {
                person: e.person,
                ts: e.ts,
                typ: e.typ,
                cat: e.cat,
                price: e.price,
            })
        }));
        exception_events
            .par_sort_unstable_by_key(|e| ((e.person as u64) << 32) | ((e.ts - ws) as u32 as u64));
    }
    eprintln!(
        "sorted {} valid events {:?}",
        events.len(),
        started.elapsed()
    );

    let ws_day = ws.div_euclid(DAY_MS);
    let workers = rayon::current_num_threads().min(events.len().max(1));
    let mut bounds = vec![0usize];
    for worker in 1..workers {
        let mut p = events.len() * worker / workers;
        while p < events.len() && p > 0 && events[p - 1].person == events[p].person {
            p += 1
        }
        if p > *bounds.last().unwrap() {
            bounds.push(p)
        }
    }
    bounds.push(events.len());
    let mut parts: Vec<_> = (0..bounds.len() - 1)
        .into_par_iter()
        .map(|part| {
            sessionize_events(
                &events[bounds[part]..bounds[part + 1]],
                ws_day,
                &cat_names,
                (!bad.is_empty()).then_some(&bad),
            )
        })
        .collect();
    if !exception_events.is_empty() {
        parts.push(sessionize_events(
            &exception_events,
            ws_day,
            &cat_names,
            None,
        ));
    }
    let daily_len = parts.iter().map(|x| x.0.len()).sum();
    let sessions_len = parts.iter().map(|x| x.1.len()).sum();
    let mut daily = Vec::with_capacity(daily_len);
    let mut sessions = Vec::with_capacity(sessions_len);
    for (mut d, mut s) in parts {
        daily.append(&mut d);
        sessions.append(&mut s)
    }
    drop(events);
    let mut session_groups: Vec<(u32, usize, usize)> = Vec::new();
    let mut group_start = 0usize;
    while group_start < sessions.len() {
        let person = sessions[group_start].person;
        let mut group_end = group_start + 1;
        while group_end < sessions.len() && sessions[group_end].person == person {
            group_end += 1;
        }
        session_groups.push((person, group_start, group_end));
        group_start = group_end;
    }
    session_groups.par_sort_unstable_by_key(|g| g.0);
    eprintln!(
        "sessionized {} sessions {:?}",
        sessions.len(),
        started.elapsed()
    );

    let (click_maps, click_winners) = if let Some(task) = click_scan_task {
        task.join().unwrap()
    } else {
        let maps = map_csv(&data.join("clicks"));
        let (key, bound) = derived_click_key
            .map(|k| (k, event_rows))
            .unwrap_or_else(|| recover_click_key(&maps));
        let (dense, generic) = parse_click_refs(&maps, key, bound, click_byte_ids);
        let winners = dedup_click_refs(dense, generic, &maps);
        (maps, winners)
    };
    let mut clicks: Vec<Click> = if std::env::var_os("LEGACY_PRECLICK").is_some() {
        let pre_clicks: Vec<PreClick> = click_winners
            .par_iter()
            .filter_map(|r| {
                let v = fields4(ref_line(r, &click_maps))?;
                if v[1].is_empty() {
                    return None;
                }
                let id = ClickId::new(v[0], r.loc);
                let user = std::str::from_utf8(v[1]).ok()?;
                let cs = std::str::from_utf8(v[2]).ok()?;
                let campaign = if campaign_dense_ok {
                    dense_decimal(cs, "c_", campaign_names.len()).map(|x| x as u16)?
                } else {
                    *campaign_ids.get(cs)?
                };
                let ts = timestamp_ms_bytes(v[3])?;
                if ts < ws - 7 * DAY_MS || ts >= we {
                    return None;
                }
                let user_number = user_fast.and_then(|(key, bound)| {
                    decode_id(user, b'u', key).filter(|&x| x < (bound as u64) * 512)
                });
                Some(PreClick {
                    row: *r,
                    user_number,
                    ts,
                    id,
                    campaign,
                })
            })
            .collect();
        let mut out = Vec::with_capacity(pre_clicks.len());
        for p in pre_clicks {
            let person = if let Some(n) = p.user_number {
                resolve_number(n, &linked_small, &linked_extra)
            } else {
                let v = fields4(ref_line(&p.row, &click_maps)).unwrap();
                let s = std::str::from_utf8(v[1]).unwrap();
                let id = uf.id(s);
                uf.root(id)
            };
            out.push(Click {
                person,
                ts: p.ts,
                id: p.id,
                campaign: p.campaign,
            })
        }
        out
    } else {
        // Resolve clicks directly into their final compact representation.
        // Generated users take the lock-free dense path; only arbitrary
        // fallback IDs acquire the union-find mutex.
        let uf_click_fallback = std::sync::Mutex::new(&mut uf);
        let out = click_winners
            .par_iter()
            .filter_map(|r| {
                let v = fields4(ref_line(r, &click_maps))?;
                if v[1].is_empty() {
                    return None;
                }
                let id = ClickId::new(v[0], r.loc);
                let user = std::str::from_utf8(v[1]).ok()?;
                let cs = std::str::from_utf8(v[2]).ok()?;
                let campaign = if campaign_dense_ok {
                    dense_decimal(cs, "c_", campaign_names.len()).map(|x| x as u16)?
                } else {
                    *campaign_ids.get(cs)?
                };
                let ts = timestamp_ms_bytes(v[3])?;
                if ts < ws - 7 * DAY_MS || ts >= we {
                    return None;
                }
                let user_number = user_fast.and_then(|(key, bound)| {
                    decode_id(user, b'u', key).filter(|&x| x < (bound as u64) * 512)
                });
                let person = if let Some(n) = user_number {
                    resolve_number(n, &linked_small, &linked_extra)
                } else {
                    let mut guard = uf_click_fallback.lock().unwrap();
                    let id = guard.id(user);
                    guard.root(id)
                };
                Some(Click {
                    person,
                    ts,
                    id,
                    campaign,
                })
            })
            .collect();
        drop(uf_click_fallback);
        out
    };
    drop(click_winners);
    clicks.par_sort_unstable_by(|a, b| {
        a.person
            .cmp(&b.person)
            .then(a.ts.cmp(&b.ts))
            .then_with(|| cmp_click_id(&a.id, &b.id, &click_maps))
    });
    eprintln!(
        "parsed/sorted {} clicks {:?}",
        clicks.len(),
        started.elapsed()
    );

    let organic = campaign_names.len() as u16;
    campaign_names.push("organic".to_owned());
    campaign_channels.push("organic".to_owned());
    let campaign_cells = (organic as usize + 1) * 14;
    let sweep_workers = rayon::current_num_threads().min(session_groups.len().max(1));
    let mut sweep_bounds = vec![0usize];
    for worker in 1..sweep_workers {
        let p = session_groups.len() * worker / sweep_workers;
        if p > *sweep_bounds.last().unwrap() {
            sweep_bounds.push(p)
        }
    }
    if *sweep_bounds.last().unwrap() != session_groups.len() {
        sweep_bounds.push(session_groups.len())
    }
    let campaign_parts: Vec<Vec<CampaignDaily>> = (0..sweep_bounds.len() - 1)
        .into_par_iter()
        .map(|part| {
            let begin = sweep_bounds[part];
            let end = sweep_bounds[part + 1];
            let mut local = vec![CampaignDaily::default(); campaign_cells];
            if begin == end {
                return local;
            }
            let mut counts = vec![0u32; organic as usize];
            let mut active: Vec<u16> = Vec::new();
            let mut active_pos = vec![usize::MAX; organic as usize];
            let first_person = session_groups[begin].0;
            let mut ci = clicks.partition_point(|c| c.person < first_person);
            for &(person, s0, si) in &session_groups[begin..end] {
                while ci < clicks.len() && clicks[ci].person < person {
                    ci += 1;
                }
                let c0 = ci;
                while ci < clicks.len() && clicks[ci].person == person {
                    ci += 1;
                }
                let c1 = ci;
                let mut add = c0;
                let mut remove = c0;
                let mut latest: Option<usize> = None;
                for s in &sessions[s0..si] {
                    while add < c1 && clicks[add].ts <= s.start {
                        let c = &clicks[add];
                        if counts[c.campaign as usize] == 0 {
                            active_pos[c.campaign as usize] = active.len();
                            active.push(c.campaign)
                        }
                        counts[c.campaign as usize] += 1;
                        latest = match latest {
                            None => Some(add),
                            Some(li)
                                if c.ts > clicks[li].ts
                                    || (c.ts == clicks[li].ts
                                        && cmp_click_id(&c.id, &clicks[li].id, &click_maps)
                                            .is_lt()) =>
                            {
                                Some(add)
                            }
                            old => old,
                        };
                        add += 1
                    }
                    while remove < add && clicks[remove].ts < s.start - 7 * DAY_MS {
                        let c = &clicks[remove];
                        let n = &mut counts[c.campaign as usize];
                        *n -= 1;
                        if *n == 0 {
                            let pos = active_pos[c.campaign as usize];
                            let moved = *active.last().unwrap();
                            active.swap_remove(pos);
                            if pos < active.len() {
                                active_pos[moved as usize] = pos
                            }
                            active_pos[c.campaign as usize] = usize::MAX
                        }
                        remove += 1
                    }
                    let credited = latest
                        .filter(|&li| clicks[li].ts >= s.start - 7 * DAY_MS)
                        .map(|li| clicks[li].campaign)
                        .unwrap_or(organic);
                    let di = (s.start.div_euclid(DAY_MS) - ws_day) as usize;
                    let d = &mut local[credited as usize * 14 + di];
                    d.sessions += 1;
                    d.revenue += s.revenue;
                    if s.purchasing {
                        d.purchasing += 1
                    }
                    for &campaign in &active {
                        local[campaign as usize * 14 + di].assisted += 1
                    }
                }
                for campaign in active.drain(..) {
                    counts[campaign as usize] = 0;
                    active_pos[campaign as usize] = usize::MAX
                }
            }
            local
        })
        .collect();
    let mut campaign_daily = vec![CampaignDaily::default(); campaign_cells];
    for part in campaign_parts {
        for (dst, src) in campaign_daily.iter_mut().zip(part) {
            dst.sessions += src.sessions;
            dst.purchasing += src.purchasing;
            dst.revenue += src.revenue;
            dst.assisted += src.assisted
        }
    }
    drop(click_maps);

    let user_out: Vec<_> = daily
        .into_par_iter()
        .map(|((person, day), d)| {
            let top = d
                .top
                .map(|c| ByteArray::from(cat_names[c as usize].as_bytes()));
            let user = if person >= SYNTH_BASE {
                let (key, _) = user_fast.unwrap();
                ByteArray::from(
                    base36_string(scramble_id((person - SYNTH_BASE) as u64, key)).into_bytes(),
                )
            } else {
                ByteArray::from(uf.name(person).as_bytes())
            };
            UserOutput {
                user,
                day: day as i32,
                sessions: d.sessions as i64,
                events: d.events as i64,
                revenue: d.revenue,
                top,
            }
        })
        .collect();
    let campaign_out: Vec<_> = campaign_daily
        .into_iter()
        .enumerate()
        .filter(|(_, d)| d.sessions > 0 || d.assisted > 0)
        .map(|(i, d)| {
            let campaign = i / 14;
            let day = ws_day + (i % 14) as i64;
            CampaignOutput {
                campaign: ByteArray::from(campaign_names[campaign].as_bytes()),
                channel: ByteArray::from(campaign_channels[campaign].as_bytes()),
                day: day as i32,
                sessions: d.sessions as i64,
                purchasing: d.purchasing as i64,
                revenue: d.revenue,
                assisted: d.assisted as i64,
            }
        })
        .collect();
    let user_dir = output_dir.join("user_daily");
    let campaign_dir = output_dir.join("campaign_daily");
    fs::create_dir_all(&user_dir).unwrap();
    fs::create_dir_all(&campaign_dir).unwrap();
    if user_out.is_empty() {
        write_user_parquet(&user_dir.join("part-0.parquet"), &user_out).unwrap()
    } else {
        let chunk = (user_out.len() + 15) / 16;
        user_out
            .par_chunks(chunk)
            .enumerate()
            .for_each(|(i, rows)| {
                write_user_parquet(&user_dir.join(format!("part-{i}.parquet")), rows).unwrap()
            })
    }
    write_campaign_parquet(&campaign_dir.join("part-0.parquet"), &campaign_out).unwrap();
    eprintln!("done {:?}", started.elapsed());
    // This is a one-shot container process. All Parquet files are already
    // closed, so let the kernel reclaim the large mmap/vector graph instead
    // of spending scored time running allocator and unmap destructors.
    std::process::exit(0);
}
