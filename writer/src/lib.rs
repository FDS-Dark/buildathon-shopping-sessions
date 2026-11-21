use parquet::basic::Compression;
use parquet::column::writer::get_typed_column_writer_mut;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use std::ffi::{c_char, CStr};
use std::fs::{self, File};
use std::path::Path;
use std::slice;
use std::sync::Arc;

const USER_SCHEMA: &str = "message user_daily { REQUIRED BINARY user_id (STRING); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (STRING); }";
const CAMPAIGN_SCHEMA: &str = "message campaign_daily { REQUIRED BINARY campaign_id (STRING); REQUIRED BINARY channel (STRING); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }";

#[repr(C)]
pub struct FeUserRow {
    pub user: *const u8,
    pub user_len: u32,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: u64,
    pub top: *const u8,
    pub top_len: u32,
}

#[repr(C)]
pub struct FeCampaignRow {
    pub campaign: *const u8,
    pub campaign_len: u32,
    pub channel: *const u8,
    pub channel_len: u32,
    pub day: i32,
    pub sessions: u32,
    pub purchasing: u32,
    pub revenue: u64,
    pub assists: u32,
}

// The caller keeps all byte buffers alive and immutable until fe_write_outputs
// returns. Distinct rows may point into the same backing string/catalog.
unsafe impl Sync for FeUserRow {}
unsafe impl Sync for FeCampaignRow {}

fn properties() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_write_batch_size(1_048_576)
            .build(),
    )
}

macro_rules! column {
    ($group:expr, $kind:ty, $values:expr, $defs:expr) => {{
        let mut writer = $group.next_column()?.expect("column");
        get_typed_column_writer_mut::<$kind>(writer.untyped())
            .write_batch($values, $defs, None)?;
        writer.close()?;
    }};
}

unsafe fn bytes(ptr: *const u8, len: u32) -> ByteArray {
    ByteArray::from(slice::from_raw_parts(ptr, len as usize).to_vec())
}

fn write_users(path: &Path, rows: &[FeUserRow]) -> parquet::errors::Result<()> {
    let mut file = SerializedFileWriter::new(
        File::create(path)?,
        Arc::new(parse_message_type(USER_SCHEMA)?),
        properties(),
    )?;
    let mut group = file.next_row_group()?;
    let users: Vec<_> = rows
        .iter()
        .map(|row| unsafe { bytes(row.user, row.user_len) })
        .collect();
    let days: Vec<_> = rows.iter().map(|row| row.day).collect();
    let sessions: Vec<_> = rows.iter().map(|row| i64::from(row.sessions)).collect();
    let events: Vec<_> = rows.iter().map(|row| i64::from(row.events)).collect();
    let revenue: Vec<_> = rows.iter().map(|row| row.revenue as i64).collect();
    let defs: Vec<i16> = rows.iter().map(|row| i16::from(!row.top.is_null())).collect();
    let top: Vec<_> = rows
        .iter()
        .filter(|row| !row.top.is_null())
        .map(|row| unsafe { bytes(row.top, row.top_len) })
        .collect();
    column!(group, ByteArrayType, &users, None);
    column!(group, Int32Type, &days, None);
    column!(group, Int64Type, &sessions, None);
    column!(group, Int64Type, &events, None);
    column!(group, Int64Type, &revenue, None);
    column!(group, ByteArrayType, &top, Some(&defs));
    group.close()?;
    file.close()?;
    Ok(())
}

fn write_campaigns(path: &Path, rows: &[FeCampaignRow]) -> parquet::errors::Result<()> {
    let mut file = SerializedFileWriter::new(
        File::create(path)?,
        Arc::new(parse_message_type(CAMPAIGN_SCHEMA)?),
        properties(),
    )?;
    let mut group = file.next_row_group()?;
    let campaigns: Vec<_> = rows
        .iter()
        .map(|row| unsafe { bytes(row.campaign, row.campaign_len) })
        .collect();
    let channels: Vec<_> = rows
        .iter()
        .map(|row| unsafe { bytes(row.channel, row.channel_len) })
        .collect();
    let days: Vec<_> = rows.iter().map(|row| row.day).collect();
    let sessions: Vec<_> = rows.iter().map(|row| i64::from(row.sessions)).collect();
    let purchasing: Vec<_> = rows.iter().map(|row| i64::from(row.purchasing)).collect();
    let revenue: Vec<_> = rows.iter().map(|row| row.revenue as i64).collect();
    let assists: Vec<_> = rows.iter().map(|row| i64::from(row.assists)).collect();
    column!(group, ByteArrayType, &campaigns, None);
    column!(group, ByteArrayType, &channels, None);
    column!(group, Int32Type, &days, None);
    column!(group, Int64Type, &sessions, None);
    column!(group, Int64Type, &purchasing, None);
    column!(group, Int64Type, &revenue, None);
    column!(group, Int64Type, &assists, None);
    group.close()?;
    file.close()?;
    Ok(())
}

fn write_all(
    output: &Path,
    users: &[FeUserRow],
    campaigns: &[FeCampaignRow],
) -> Result<(), String> {
    let user_dir = output.join("user_daily");
    let campaign_dir = output.join("campaign_daily");
    fs::create_dir_all(&user_dir).map_err(|e| e.to_string())?;
    fs::create_dir_all(&campaign_dir).map_err(|e| e.to_string())?;
    let user_chunk = users.len().div_ceil(32).max(1);
    users
        .par_chunks(user_chunk)
        .enumerate()
        .try_for_each(|(i, rows)| {
            write_users(&user_dir.join(format!("part-{i}.parquet")), rows)
        })
        .map_err(|e| e.to_string())?;
    let campaign_chunk = campaigns.len().div_ceil(8).max(1);
    campaigns
        .par_chunks(campaign_chunk)
        .enumerate()
        .try_for_each(|(i, rows)| {
            write_campaigns(&campaign_dir.join(format!("part-{i}.parquet")), rows)
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Writes exact scorer Parquet schemas directly from C/C++ aggregate rows.
///
/// Returns 0 on success, 1 for invalid pointers/path, and 2 for I/O/Parquet
/// failure. The function does not retain any caller pointer after it returns.
#[no_mangle]
pub unsafe extern "C" fn fe_write_outputs(
    output_dir: *const c_char,
    users: *const FeUserRow,
    user_count: usize,
    campaigns: *const FeCampaignRow,
    campaign_count: usize,
) -> i32 {
    if output_dir.is_null()
        || (user_count != 0 && users.is_null())
        || (campaign_count != 0 && campaigns.is_null())
    {
        return 1;
    }
    let path = match CStr::from_ptr(output_dir).to_str() {
        Ok(value) => Path::new(value),
        Err(_) => return 1,
    };
    let user_rows = slice::from_raw_parts(users, user_count);
    let campaign_rows = slice::from_raw_parts(campaigns, campaign_count);
    match write_all(path, user_rows, campaign_rows) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("fastengine parquet writer: {error}");
            2
        }
    }
}


