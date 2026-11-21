#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <future>
#include <immintrin.h>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include "identity.hpp"
#include "fastengine_parquet.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

constexpr uint64_t DAY_MS = 86'400'000;
constexpr uint64_t ID_MASK = (1ULL << 40) - 1;
constexpr uint64_t ID_A = 0x9E37'79B9'7F4BULL;
constexpr uint64_t ID_B = 0xD6E8'FEB8'6659ULL;
constexpr size_t CHUNK = 2u << 20;
constexpr size_t OVERLAP = 64u << 10;
constexpr unsigned PARTITIONS = 64;
constexpr unsigned DEDUP_OWNERS = 512;

// Final common-path semantic record. Purchase product IDs live in a sparse side
// stream because non-purchase products never affect the answer.
struct CompactEvent {
  uint32_t user_index;
  uint32_t ts_rel;
  uint32_t meta;  // price[0..23], type[24..25], flags[26..31]
};
static_assert(sizeof(CompactEvent) == 12);

// Exact dedup auxiliary. It is intentionally separate from CompactEvent so the
// session stream stays 16 bytes and can be consumed without event-id traffic.
struct __attribute__((packed)) DenseAux {
  uint32_t dense_index;
  uint32_t location;
  uint32_t ingest_lo;
  uint8_t ingest_hi;
};
static_assert(sizeof(DenseAux) == 13);

static inline uint64_t aux_ingest(DenseAux const &a) {
  return uint64_t(a.ingest_lo) | (uint64_t(a.ingest_hi) << 32);
}

static inline DenseAux make_aux(uint32_t dense_index, uint32_t location, int64_t ingest_ms) {
  // Calendar years 0000..9999 span fewer than 2^39 whole seconds. Offset year
  // 0000 to zero and store the exact F1 second in five bytes.
  constexpr int64_t YEAR_ZERO_TO_EPOCH_S = 62'167'219'200LL;
  uint64_t seconds = uint64_t(ingest_ms / 1000 + YEAR_ZERO_TO_EPOCH_S);
  if (seconds >= (1ULL << 40)) std::abort();
  return DenseAux{dense_index, location, uint32_t(seconds), uint8_t(seconds >> 32)};
}

struct ProductExtra {
  uint64_t product_key;
  uint32_t location;
  uint32_t reserved;
};

struct GenericEvent {
  std::array<std::string, 7> fields;
  uint32_t location;
  bool generic_id;
  bool semantic_valid;
  bool winner = false;
};

struct Task {
  fs::path path;
  uint64_t file_size;
  uint64_t begin;
  uint64_t end;
};

struct Stats {
  uint64_t physical = 0;
  uint64_t structural_bad = 0;
  uint64_t empty_id = 0;
  uint64_t semantic_bad = 0;
  uint64_t compact = 0;
  uint64_t valid_compact = 0;
  uint64_t generic = 0;
  uint64_t purchases = 0;
  uint64_t boundary_grows = 0;
  uint64_t checksum = 0;
};

struct Worker {
  std::array<std::vector<CompactEvent>, PARTITIONS> events;
  std::array<std::array<CompactEvent, 4>, PARTITIONS> event_stage{};
  std::array<std::vector<DenseAux>, DEDUP_OWNERS> dedup;
  std::array<std::array<DenseAux, 4>, DEDUP_OWNERS> dedup_stage{};
  std::array<uint8_t, DEDUP_OWNERS> dedup_stage_used{};
  std::array<uint8_t, PARTITIONS> event_stage_used{};
  std::vector<ProductExtra> products;
  std::vector<uint32_t> high_alias_locations;
  std::vector<GenericEvent> generic;
  Stats stats;

  uint32_t staged_index(unsigned part) const {
    return uint32_t(events[part].size() + event_stage_used[part]);
  }
  void push_event(unsigned part, CompactEvent value) {
    uint8_t &used = event_stage_used[part];event_stage[part][used]=value;++used;if(used==4){events[part].insert(events[part].end(),event_stage[part].begin(),event_stage[part].end());used=0;}
  }
  void push_dedup(unsigned owner, DenseAux value) {
    uint8_t &used=dedup_stage_used[owner];dedup_stage[owner][used++]=value;if(used==4){dedup[owner].insert(dedup[owner].end(),dedup_stage[owner].begin(),dedup_stage[owner].end());used=0;}
  }
  void finish_events() {
    for(unsigned p=0;p<PARTITIONS;++p){uint8_t&u=event_stage_used[p];events[p].insert(events[p].end(),event_stage[p].begin(),event_stage[p].begin()+u);u=0;}for(unsigned o=0;o<DEDUP_OWNERS;++o){uint8_t&u=dedup_stage_used[o];dedup[o].insert(dedup[o].end(),dedup_stage[o].begin(),dedup_stage[o].begin()+u);u=0;}
  }
};

struct Fields7 {
  std::array<std::string_view, 7> v;
};

static inline uint64_t mix64(uint64_t x) {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

static uint64_t inv_odd(uint64_t x) {
  uint64_t y = x;
  for (unsigned i = 0; i < 6; ++i) y *= 2 - x * y;
  return y;
}

static uint64_t id_post(uint64_t y) {
  uint64_t x = y * inv_odd(ID_B) & ID_MASK;
  x ^= x >> 17;
  x ^= x >> 34;
  return x & ID_MASK;
}

static uint64_t decode_number(uint64_t number, uint64_t key) {
  return (id_post(number) ^ key) * inv_odd(ID_A) & ID_MASK;
}

static uint64_t scramble_number(uint64_t value, uint64_t key) {
  uint64_t x = value & ID_MASK;
  x = (x * ID_A ^ key) & ID_MASK;
  x ^= x >> 17;
  return x * ID_B & ID_MASK;
}

static uint64_t packed_number(uint64_t packed) {
  return packed & ((1ULL << 48) - 1);
}

struct RecoveredKey { uint64_t key = 0; bool valid = false; };

static RecoveredKey recover_event_key(const std::vector<uint64_t> &numbers, uint32_t bound) {
  if (numbers.size() < 4 || bound < 100'000) return {};
  const uint64_t anchor = id_post(numbers[0]);
  uint64_t found = 0;
  bool have = false;
  for (uint64_t i = 0; i < bound; ++i) {
    uint64_t key = anchor ^ (i * ID_A & ID_MASK);
    bool ok = true;
    for (size_t j = 1; j < numbers.size(); ++j) {
      if (decode_number(numbers[j], key) >= bound) { ok = false; break; }
    }
    if (ok) {
      if (have && found != key) return {};
      found = key;
      have = true;
    }
  }
  return {found, have};
}

static RecoveredKey recover_user_key(const std::vector<uint64_t> &numbers, uint32_t person_bound) {
  if (numbers.size() < 8 || person_bound < 20) return {};
  uint64_t limit = uint64_t(person_bound) * 512;
  for (size_t anchor_i = 0; anchor_i < std::min<size_t>(6, numbers.size()); ++anchor_i) {
    uint64_t anchor = id_post(numbers[anchor_i]);
    for (uint64_t person = 0; person < person_bound; ++person) {
      for (uint64_t alias = 0; alias < 9; ++alias) {
        uint64_t key = anchor ^ ((person * 512 + alias) * ID_A & ID_MASK);
        bool ok = true;
        for (uint64_t number : numbers) {
          uint64_t q = decode_number(number, key);
          if (!(q < limit || (q >= (1ULL << 39) && q < (1ULL << 39) + 16))) {
            ok = false; break;
          }
        }
        if (ok) return {key, true};
      }
    }
  }
  return {};
}

// Lossless common-ID representation: prefix, exact digit count and base36
// value. parse_packed rejects every non-canonical spelling, so unpacking is
// byte-identical. The remaining spellings go to the exact string fallback.
static const std::array<uint8_t, 256> B36 = [] {
  std::array<uint8_t, 256> t{};
  t.fill(0xff);
  for (unsigned i = 0; i < 10; ++i) t['0' + i] = uint8_t(i);
  for (unsigned i = 0; i < 26; ++i) t['a' + i] = uint8_t(i + 10);
  return t;
}();

static bool parse_packed(std::string_view s, char prefix, uint64_t &out) {
  if (s.size() < 2 || s.size() > 9 || s[0] != prefix) return false;
  const size_t n = s.size() - 1;
  if (n > 1 && s[1] == '0') return false;
  if (s.size() == 9) {
    uint8_t d0=B36[uint8_t(s[1])], d1=B36[uint8_t(s[2])], d2=B36[uint8_t(s[3])], d3=B36[uint8_t(s[4])];
    uint8_t d4=B36[uint8_t(s[5])], d5=B36[uint8_t(s[6])], d6=B36[uint8_t(s[7])], d7=B36[uint8_t(s[8])];
    if ((d0|d1|d2|d3|d4|d5|d6|d7) == 0xff) return false;
    uint64_t value=d0; value=value*36+d1; value=value*36+d2; value=value*36+d3;
    value=value*36+d4; value=value*36+d5; value=value*36+d6; value=value*36+d7;
    if (value > ID_MASK) return false;
    out = (uint64_t(uint8_t(prefix)) << 56) | (uint64_t(n) << 52) | value;
    return true;
  }
  uint64_t value = 0;
  for (size_t i = 1; i < s.size(); ++i) {
    unsigned d;
    unsigned char c = static_cast<unsigned char>(s[i]);
    if (c >= '0' && c <= '9') d = c - '0';
    else if (c >= 'a' && c <= 'z') d = c - 'a' + 10;
    else return false;
    if (value > (ID_MASK - d) / 36) return false;
    value = value * 36 + d;
  }
  if (value > ID_MASK) return false;
  out = (uint64_t(uint8_t(prefix)) << 56) | (uint64_t(n) << 52) | value;
  return true;
}

static std::string unpack_packed(uint64_t x) {
  char prefix = char(x >> 56);
  unsigned n = unsigned((x >> 52) & 0xf);
  uint64_t value = x & ((1ULL << 48) - 1);
  std::string out(n + 1, '0');
  out[0] = prefix;
  for (unsigned i = 0; i < n; ++i) {
    unsigned d = unsigned(value % 36);
    value /= 36;
    out[n - i] = char(d < 10 ? '0' + d : 'a' + d - 10);
  }
  if (value != 0) return {};
  return out;
}
#if 0

struct CampaignCatalog{std::vector<std::string> ids,channels;std::unordered_map<std::string,uint16_t> map;std::vector<int16_t> numeric;};
static CampaignCatalog load_campaigns(fs::path const&data){CampaignCatalog c;std::ifstream in(data/"campaigns.csv",std::ios::binary);std::string line;std::getline(in,line);while(std::getline(in,line)){std::string_view a,b;if(!IdentityMap::split2(line,a,b)||a.empty())continue;uint16_t ix=uint16_t(c.ids.size());c.ids.emplace_back(a);c.channels.emplace_back(b);c.map.emplace(c.ids.back(),ix);if(a.size()>2&&a[0]=='c'&&a[1]=='_'){uint64_t n=0;bool ok=true;for(size_t i=2;i<a.size();++i){if(a[i]<'0'||a[i]>'9'){ok=false;break;}n=n*10+a[i]-'0';}if(ok&&n<1'000'000){if(c.numeric.size()<=n)c.numeric.resize(n+1,-1);c.numeric[n]=int16_t(ix);}}}return c;}
struct ClickRec{uint32_t person,ts;uint16_t campaign,flags;uint64_t id;};
struct ClickStats{uint64_t physical=0,bad=0,empty=0,invalid=0,valid=0,credited=0,assisted=0,revenue=0;double scan_s=0,tail_s=0;};
static bool split4(std::string_view line,std::array<std::string_view,4>&f){if(line.empty())return false;size_t s=0;for(unsigned i=0;i<4;++i){size_t e=i==3?line.size():line.find(',',s);if(e==line.npos)return false;auto x=line.substr(s,e-s);if(!x.empty()&&x.front()=='"'){if(x.size()<2||x.back()!='"')return false;x.remove_prefix(1);x.remove_suffix(1);if(x.find('"')!=x.npos)return false;}else if(x.find('"')!=x.npos)return false;f[i]=x;s=e+1;}return s==line.size()+1;}
static int campaign_lookup(CampaignCatalog const&c,std::string_view s){if(s.size()>2&&s[0]=='c'&&s[1]=='_'){uint64_t n=0;bool ok=true;for(size_t i=2;i<s.size();++i){if(s[i]<'0'||s[i]>'9'){ok=false;break;}n=n*10+s[i]-'0';}if(ok&&n<c.numeric.size()&&c.numeric[n]>=0)return c.numeric[n];}auto it=c.map.find(std::string(s));return it==c.map.end()?-1:it->second;}
static bool packed_lex_less(uint64_t a,uint64_t b){char ab[13],bb[13];return IdentityMap::packed_view(a,ab)<IdentityMap::packed_view(b,bb);}
struct ClickBundle{std::vector<ClickRec> rows;std::vector<std::string> generic;CampaignCatalog campaigns;ClickStats stats;};
static ClickBundle scan_clicks(fs::path const&data,int ncpu,int64_t ws,int64_t we,uint64_t user_key,uint32_t person_bound,IdentityMap const&id,std::vector<uint32_t>const&session_offsets){
  auto started=Clock::now();ClickBundle out;out.campaigns=load_campaigns(data);struct CW{std::vector<ClickRec> rows;std::vector<std::string> generic;ClickStats s;};std::vector<CW>w(ncpu);std::vector<Task>tasks;for(auto const&e:fs::directory_iterator(data/"clicks"))if(e.is_regular_file()&&e.path().extension()==".csv"){uint64_t n=e.file_size();for(uint64_t b=0;b<n;b+=CHUNK)tasks.push_back({e.path(),n,b,std::min<uint64_t>(n,b+CHUNK)});}std::atomic<size_t>next{0};std::vector<std::thread>th;
  auto code_dense=[&](uint64_t q){uint32_t r=id.resolve_dense(q);if(r!=UINT32_MAX)return r;uint64_t p=q>>9,a=q&511;if(p<person_bound&&a<9)return g_linked_base+uint32_t(p*9+a);auto it=g_dense_extra_codes.find(q);return it==g_dense_extra_codes.end()?UINT32_MAX:it->second;};
  for(int tid=0;tid<ncpu;++tid)th.emplace_back([&,tid]{DateCache dc;auto&x=w[tid];x.rows.reserve(700'000);while(true){size_t ti=next.fetch_add(1);if(ti>=tasks.size())break;std::vector<char>buf;if(!load_task(tasks[ti],buf,x.s))std::abort();size_t pos=0;if(tasks[ti].begin==0){auto p=std::find(buf.begin(),buf.end(),'\n');if(p==buf.end())continue;pos=size_t(p-buf.begin())+1;}else{char prev=0;int fd=open(tasks[ti].path.c_str(),O_RDONLY);pread(fd,&prev,1,tasks[ti].begin-1);close(fd);if(prev!='\n'){auto p=std::find(buf.begin(),buf.end(),'\n');if(p==buf.end())continue;pos=size_t(p-buf.begin())+1;}}while(pos<buf.size()){size_t e=pos;while(e<buf.size()&&buf[e]!='\n')++e;if(tasks[ti].begin+pos>=tasks[ti].end)break;if(e==buf.size()&&tasks[ti].end<tasks[ti].file_size)break;++x.s.physical;std::array<std::string_view,4>f;if(!split4({buf.data()+pos,e-pos},f)){++x.s.bad;pos=e+1;continue;}if(f[0].empty()){++x.s.empty;pos=e+1;continue;}int64_t tm=0;int camp=campaign_lookup(out.campaigns,f[2]);bool valid=!f[1].empty()&&fast_ts(f[3],dc,tm)&&tm>=ws-int64_t(7*DAY_MS)&&tm<we&&camp>=0;if(!valid){++x.s.invalid;pos=e+1;continue;}uint32_t person=UINT32_MAX;uint64_t up=0;if(parse_packed(f[1],'u',up)){person=code_dense(decode_number(packed_number(up),user_key));}else{auto li=id.generic_nodes.find(std::string(f[1]));if(li!=id.generic_nodes.end())person=id.parent[li->second];else{auto gi=g_generic_extra_codes.find(std::string(f[1]));if(gi!=g_generic_extra_codes.end())person=gi->second;}}if(person==UINT32_MAX||person>=session_offsets.size()-1||session_offsets[person]==session_offsets[person+1]){pos=e+1;continue;}uint64_t kp=0;uint16_t flags=0;uint64_t kid=0;if(parse_packed(f[0],'k',kp))kid=kp;else{flags=1;kid=x.generic.size();x.generic.emplace_back(f[0]);}x.rows.push_back({person,uint32_t(tm-(ws-int64_t(7*DAY_MS))),uint16_t(camp),flags,kid});++x.s.valid;pos=e+1;}}});for(auto&t:th)t.join();
  size_t total=0;for(auto&x:w)total+=x.rows.size();out.rows.reserve(total);for(auto&x:w){uint64_t base=out.generic.size();out.generic.insert(out.generic.end(),std::make_move_iterator(x.generic.begin()),std::make_move_iterator(x.generic.end()));for(auto&r:x.rows){if(r.flags)r.id+=base;out.rows.push_back(r);}out.stats.physical+=x.s.physical;out.stats.bad+=x.s.bad;out.stats.empty+=x.s.empty;out.stats.invalid+=x.s.invalid;out.stats.valid+=x.s.valid;}out.stats.scan_s=std::chrono::duration<double>(Clock::now()-started).count();return out;
}

#endif
static bool split7(std::string_view line, Fields7 &out) {
  if (line.empty()) return false;
  size_t start = 0;
  unsigned field = 0;
  for (size_t i = 0; i <= line.size(); ++i) {
    if (i != line.size() && line[i] != ',') continue;
    if (field >= 7) return false;
    std::string_view raw = line.substr(start, i - start);
    if (!raw.empty() && raw.front() == '"') {
      if (raw.size() < 2 || raw.back() != '"') return false;
      raw.remove_prefix(1);
      raw.remove_suffix(1);
      if (raw.find('"') != std::string_view::npos) return false;
    } else if (raw.find('"') != std::string_view::npos) {
      return false;
    }
    out.v[field++] = raw;
    start = i + 1;
  }
  return field == 7;
}

static std::vector<uint64_t> sample_numbers(const fs::path &events, unsigned field, char prefix, size_t wanted) {
  std::vector<uint64_t> out;
  std::vector<fs::path> paths;
  for (auto const &entry : fs::directory_iterator(events))
    if (entry.is_regular_file() && entry.path().extension() == ".csv") paths.push_back(entry.path());
  std::sort(paths.begin(), paths.end());
  for (auto const &path : paths) {
    std::ifstream in(path, std::ios::binary);
    std::string line;
    std::getline(in, line); // header
    while (out.size() < wanted && std::getline(in, line)) {
      Fields7 f;
      uint64_t packed = 0;
      if (split7(line, f) && !f.v[field].empty() && parse_packed(f.v[field], prefix, packed))
        out.push_back(packed_number(packed));
    }
    if (out.size() >= wanted) break;
  }
  return out;
}

static bool leap(int y) {
  return (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0);
}

// Howard Hinnant's civil-date transform, derived independently from the
// Gregorian calendar rules. Returns days relative to 1970-01-01.
static int64_t days_from_civil(int y, unsigned m, unsigned d) {
  y -= m <= 2;
  const int era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = unsigned(y - era * 400);
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return int64_t(era) * 146097 + int64_t(doe) - 719468;
}

static bool digits(std::string_view s, int &v) {
  v = 0;
  if (s.empty()) return false;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + c - '0';
  }
  return true;
}

static inline int dec2(const char *p) {
  unsigned a = unsigned(uint8_t(p[0]) - uint8_t('0'));
  unsigned b = unsigned(uint8_t(p[1]) - uint8_t('0'));
  return (a <= 9 && b <= 9) ? int(a * 10 + b) : -1;
}

static bool parse_calendar(std::string_view s, int64_t &ms) {
  const bool f1 = s.size() == 20 && s[19] == 'Z';
  const bool f2 = s.size() == 25 && (s[19] == '+' || s[19] == '-') && s[22] == ':';
  if (!f1 && !f2) return false;
  if (s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;
  unsigned y0=unsigned(uint8_t(s[0])-uint8_t('0')), y1=unsigned(uint8_t(s[1])-uint8_t('0'));
  unsigned y2=unsigned(uint8_t(s[2])-uint8_t('0')), y3=unsigned(uint8_t(s[3])-uint8_t('0'));
  if ((y0|y1|y2|y3) > 9) return false;
  int y=int(y0*1000+y1*100+y2*10+y3), mo=dec2(s.data()+5), da=dec2(s.data()+8);
  int hh=dec2(s.data()+11), mm=dec2(s.data()+14), ss=dec2(s.data()+17);
  static constexpr int mdays[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
  if (mo < 1 || mo > 12 || da < 1 || da > mdays[mo] + (mo == 2 && leap(y)) ||
      hh > 23 || mm > 59 || ss > 59) return false;
  int off = 0;
  if (f2) {
    int oh=dec2(s.data()+20), om=dec2(s.data()+23);
    if (oh < 0 || om < 0 || oh > 23 || om > 59) return false;
    off = (oh * 60 + om) * (s[19] == '+' ? 1 : -1);
  }
  __int128 sec = (__int128(days_from_civil(y, unsigned(mo), unsigned(da))) * 86400 + hh * 3600 + mm * 60 + ss) - int64_t(off) * 60;
  __int128 val = sec * 1000;
  if (val < std::numeric_limits<int64_t>::min() || val > std::numeric_limits<int64_t>::max()) return false;
  ms = int64_t(val);
  return true;
}

static bool parse_ts(std::string_view s, int64_t &ms) {
  if (parse_calendar(s, ms)) return true;
  if (s.empty()) return false;
  uint64_t value = 0;
  bool overflow = false;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    unsigned d = unsigned(c - '0');
    if (value > (uint64_t(std::numeric_limits<int64_t>::max()) - d) / 10) overflow = true;
    else if (!overflow) value = value * 10 + d;
  }
  if (overflow) return false;
  ms = int64_t(value);
  return true;
}

struct DateCell { uint32_t key=UINT32_MAX; int32_t day=0; uint8_t valid=0; };
struct DateCache { std::array<DateCell,256> c{}; DateCache(){for(auto&x:c)x.key=UINT32_MAX;} };
static inline bool cached_date(const char*p,DateCache&dc,int64_t&day){unsigned y0=p[0]-'0',y1=p[1]-'0',y2=p[2]-'0',y3=p[3]-'0';if((y0|y1|y2|y3)>9)return false;int y=int(y0*1000+y1*100+y2*10+y3),m=dec2(p+5),d=dec2(p+8);if(m<0||d<0)return false;uint32_t key=uint32_t(y*512+m*32+d);auto&x=dc.c[(key*2654435761u)>>24];if(x.key!=key){static constexpr int md[]={0,31,28,31,30,31,30,31,31,30,31,30,31};x.key=key;x.valid=uint8_t(m>=1&&m<=12&&d>=1&&d<=md[m]+(m==2&&leap(y)));if(x.valid)x.day=int32_t(days_from_civil(y,unsigned(m),unsigned(d)));}if(!x.valid)return false;day=x.day;return true;}
static inline bool fast_calendar(std::string_view s,DateCache&dc,int64_t&ms){bool f1=s.size()==20&&s[19]=='Z',f2=s.size()==25&&(s[19]=='+'||s[19]=='-')&&s[22]==':';if(!f1&&!f2)return false;auto p=s.data();if(p[4]!='-'||p[7]!='-'||p[10]!='T'||p[13]!=':'||p[16]!=':')return false;int h=dec2(p+11),m=dec2(p+14),z=dec2(p+17);int64_t day;if(h<0||h>23||m<0||m>59||z<0||z>59||!cached_date(p,dc,day))return false;int off=0;if(f2){int oh=dec2(p+20),om=dec2(p+23);if(oh<0||oh>23||om<0||om>59)return false;off=(oh*60+om)*(p[19]=='+'?1:-1);}ms=(day*86400+h*3600+m*60+z-int64_t(off)*60)*1000;return true;}
static inline bool fast_ts(std::string_view s,DateCache&dc,int64_t&ms){if(s.size()==20||s.size()==25)return fast_calendar(s,dc,ms);if(s.size()==13){uint64_t x=0;for(char c:s){unsigned d=unsigned(uint8_t(c)-uint8_t('0'));if(d>9)return false;x=x*10+d;}ms=int64_t(x);return true;}return parse_ts(s,ms);}
static inline bool fast_ingest(std::string_view s,DateCache&dc,int64_t&ms){int64_t day;if(s.size()!=20||!cached_date(s.data(),dc,day))return false;auto p=s.data();int h=(p[11]-'0')*10+p[12]-'0',m=(p[14]-'0')*10+p[15]-'0',z=(p[17]-'0')*10+p[18]-'0';ms=(day*86400+h*3600+m*60+z)*1000;return true;}

static bool parse_price(std::string_view s, uint32_t &out) {
  if (s.empty()) return false;
  uint64_t v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + unsigned(c - '0');
    if (v > 10'000'000) return false;
  }
  if (v == 0) return false;
  out = uint32_t(v);
  return true;
}

static int event_type(std::string_view s) {
  if (s == "view") return 0;
  if (s == "cart") return 1;
  if (s == "purchase") return 2;
  return -1;
}

static std::string read_file(const fs::path &p) {
  std::ifstream in(p, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(in), {});
}

static bool manifest_window(const fs::path &data, int64_t &ws, int64_t &we) {
  std::string s = read_file(data / "manifest.json");
  auto value = [&](std::string_view key, int64_t &dst) {
    size_t p = s.find(key);
    if (p == std::string::npos) return false;
    p = s.find(':', p + key.size());
    if (p == std::string::npos) return false;
    p = s.find('"', p + 1);
    if (p == std::string::npos) return false;
    size_t q = s.find('"', p + 1);
    if (q == std::string::npos) return false;
    return parse_calendar(std::string_view(s).substr(p + 1, q - p - 1), dst);
  };
  return value("window_start", ws) && value("window_end", we);
}

static inline uint32_t encode_location(unsigned worker, unsigned part, uint32_t index) {
  if (worker >= 8 || part >= 64 || index >= (1u << 23)) std::abort();
  return (worker << 29) | (part << 23) | index;
}

static void process_fields(Fields7 const &f, Worker &w, unsigned worker, int64_t ws, int64_t we,
                           bool verify_pack, int bench_mode, uint64_t dense_key, uint32_t dense_bound,
                           bool dense_ready, uint64_t user_dense_key, uint32_t user_dense_bound,
                           bool user_dense_ready, bool fingerprints) {
  if (f.v[0].empty()) { ++w.stats.empty_id; return; }
  if (bench_mode == 1) return;

  uint64_t event_key = 0, user_key = 0, product_key = 0;
  bool ce = parse_packed(f.v[0], 'e', event_key);
  bool cu = parse_packed(f.v[1], 'u', user_key);
  bool cp = false;
  if (verify_pack) {
    if (ce && unpack_packed(event_key) != f.v[0]) std::abort();
    if (cu && unpack_packed(user_key) != f.v[1]) std::abort();
  }
  uint32_t dense_index = 0;
  bool event_dense = ce && dense_ready;
  if (event_dense) {
    uint64_t number = packed_number(event_key);
    uint64_t decoded = decode_number(number, dense_key);
    event_dense = decoded < dense_bound && scramble_number(decoded, dense_key) == number;
    dense_index = uint32_t(decoded);
  }
  uint32_t user_index = uint32_t(mix64(user_key));
  if (cu && user_dense_ready) {
    uint64_t number = packed_number(user_key);
    uint64_t decoded = decode_number(number, user_dense_key);
    cu = decoded < user_dense_bound && scramble_number(decoded, user_dense_key) == number;
    user_index = uint32_t(decoded);
  }

  int64_t ts = 0, ingest = 0;
  int ty = event_type(f.v[4]);
  if (ty == 2) cp = parse_packed(f.v[5], 'p', product_key);
  if (verify_pack && cp && unpack_packed(product_key) != f.v[5]) std::abort();
  uint32_t price = 0;
  thread_local DateCache date_cache;
  bool ingest_ok = fast_ingest(f.v[3], date_cache, ingest);
  bool ok = !f.v[1].empty() && fast_ts(f.v[2], date_cache, ts) && ingest_ok &&
            ts >= ws && ts < we && ty >= 0 && !f.v[5].empty() &&
            (ty != 2 || parse_price(f.v[6], price)) && ingest - ts <= int64_t(DAY_MS);
  if (!ok) ++w.stats.semantic_bad;

  if (bench_mode == 2) {
    if (!ce || !ingest_ok) ++w.stats.generic;
    else {
      ++w.stats.compact;
      if (ok) ++w.stats.valid_compact;
      if (!cu || (ok && ty == 2 && !cp)) ++w.stats.generic;
      if (ok && ty == 2 && cp) ++w.stats.purchases;
      uint32_t rel = ok ? uint32_t(uint64_t(ts - ws)) : 0;
      uint32_t meta = (price & 0x00ffffffu) | (uint32_t(std::max(ty, 0)) << 24) |
                      (ok ? (1u << 31) : 0) | ((!cu || (ty == 2 && !cp)) ? (1u << 30) : 0);
      if (fingerprints)
        w.stats.checksum ^= mix64(event_key ^ (user_key << 1) ^ uint64_t(rel) ^ (uint64_t(meta) << 32));
    }
    return;
  }

  // A generic event ID is owned entirely by the exact fallback. It still
  // participates in dedup-before-validation there, including invalid copies.
  if (!event_dense || !ingest_ok) {
    GenericEvent g{};
    for (unsigned i = 0; i < 7; ++i) g.fields[i] = std::string(f.v[i]);
    g.location = std::numeric_limits<uint32_t>::max();
    g.generic_id = true;
    g.semantic_valid = ok;
    w.generic.push_back(std::move(g));
    if (!ce || !cu || (ok && ty == 2 && !cp)) ++w.stats.generic;
    if (ce) {
      ++w.stats.compact;
      if (ok) ++w.stats.valid_compact;
      uint32_t rel = ok ? uint32_t(uint64_t(ts - ws)) : 0;
      uint32_t meta = (price & 0x00ffffffu) | (uint32_t(std::max(ty, 0)) << 24) |
                      (ok ? (1u << 31) : 0) | ((!cu || (ty == 2 && !cp)) ? (1u << 30) : 0);
      if (fingerprints)
        w.stats.checksum ^= mix64(event_key ^ (user_key << 1) ^ uint64_t(rel) ^
                                  (uint64_t(meta) << 32));
    }
    return;
  }

  // Common event IDs always enter the compact dedup stream, even when their
  // chosen copy is semantically invalid. This preserves dedup-before-validation.
  unsigned part = unsigned(uint32_t((cu ? user_index : dense_index) * 2654435761u) >> 26);
  uint32_t idx = w.staged_index(part);
  uint32_t loc = encode_location(worker, part, idx);
  uint32_t rel = ok ? uint32_t(uint64_t(ts - ws)) : 0;
  uint32_t meta = (price & 0x00ffffffu) | (uint32_t(std::max(ty, 0)) << 24);
  if (ok) meta |= 1u << 31;
  if (!cu || (ty == 2 && !cp)) meta |= 1u << 30;
  w.push_event(part, CompactEvent{cu ? user_index : 0, rel, meta});
  if(cu&&((user_index&511)>=9))w.high_alias_locations.push_back(loc);
  unsigned owner = unsigned(uint64_t(dense_index) * DEDUP_OWNERS / dense_bound);
  w.push_dedup(owner, make_aux(dense_index, loc, ingest));
  if (ok && ty == 2 && cp) {
    w.products.push_back(ProductExtra{product_key, loc, 0});
    ++w.stats.purchases;
  }
  if (!cu || (ok && ty == 2 && !cp)) {
    GenericEvent g{};
    for (unsigned i = 0; i < 7; ++i) g.fields[i] = std::string(f.v[i]);
    g.location = loc;
    g.generic_id = false;
    g.semantic_valid = ok;
    w.generic.push_back(std::move(g));
    ++w.stats.generic;
  }
  ++w.stats.compact;
  if (ok) ++w.stats.valid_compact;
  if (fingerprints)
    w.stats.checksum ^= mix64(event_key ^ (user_key << 1) ^ uint64_t(rel) ^ (uint64_t(meta) << 32));
}

static void process_line(std::string_view line, Worker &w, unsigned worker, int64_t ws, int64_t we,
                         bool verify_pack, int bench_mode, uint64_t dense_key, uint32_t dense_bound,
                         bool dense_ready, uint64_t user_dense_key, uint32_t user_dense_bound,
                         bool user_dense_ready, bool fingerprints) {
  ++w.stats.physical;
  Fields7 f;
  if (!split7(line, f)) { ++w.stats.structural_bad; return; }
  process_fields(f, w, worker, ws, we, verify_pack, bench_mode, dense_key, dense_bound,
                 dense_ready, user_dense_key, user_dense_bound, user_dense_ready, fingerprints);
}

static inline void append_mask(uint64_t out[4], uint64_t bits, unsigned offset) {
  if (offset >= 256) return;
  unsigned word = offset >> 6, shift = offset & 63;
  out[word] |= bits << shift;
  if (shift && word + 1 < 4) out[word + 1] |= bits >> (64 - shift);
}

static inline unsigned mask_pop(const uint64_t x[4]) {
  return unsigned(__builtin_popcountll(x[0]) + __builtin_popcountll(x[1]) +
                  __builtin_popcountll(x[2]) + __builtin_popcountll(x[3]));
}

static inline unsigned mask_take_first(uint64_t x[4]) {
  for (unsigned word = 0; word < 4; ++word) if (x[word]) {
    unsigned bit = unsigned(__builtin_ctzll(x[word]));
    x[word] &= x[word] - 1;
    return word * 64 + bit;
  }
  return 256;
}

static void consume_masked_row(const char *line, unsigned n, uint64_t commas[4], uint64_t quotes[4],
                               Worker &w, unsigned worker, int64_t ws, int64_t we, bool verify_pack,
                               int bench_mode, uint64_t dense_key, uint32_t dense_bound,
                               bool dense_ready, uint64_t user_dense_key, uint32_t user_dense_bound,
                               bool user_dense_ready, bool fingerprints) {
  ++w.stats.physical;
  if (n > 256) {
    Fields7 f;
    if (!split7(std::string_view(line, n), f)) { ++w.stats.structural_bad; return; }
    process_fields(f, w, worker, ws, we, verify_pack, bench_mode, dense_key, dense_bound,
                   dense_ready, user_dense_key, user_dense_bound, user_dense_ready, fingerprints);
    return;
  }
  if (!n || mask_pop(commas) != 6) { ++w.stats.structural_bad; return; }
  uint64_t pending[4] = {commas[0], commas[1], commas[2], commas[3]};
  uint64_t expected_quotes[4] = {0,0,0,0};
  unsigned ends[7];
  for (unsigned i = 0; i < 6; ++i) ends[i] = mask_take_first(pending);
  ends[6] = n;
  Fields7 f;
  unsigned start = 0;
  for (unsigned i = 0; i < 7; ++i) {
    unsigned end = ends[i];
    if (end < start || end > n) { ++w.stats.structural_bad; return; }
    if (end > start && line[start] == '"') {
      if (end - start < 2 || line[end - 1] != '"') { ++w.stats.structural_bad; return; }
      expected_quotes[start >> 6] |= 1ULL << (start & 63);
      expected_quotes[(end - 1) >> 6] |= 1ULL << ((end - 1) & 63);
      f.v[i] = std::string_view(line + start + 1, end - start - 2);
    } else {
      f.v[i] = std::string_view(line + start, end - start);
    }
    start = end + 1;
  }
  if (quotes[0] != expected_quotes[0] || quotes[1] != expected_quotes[1] ||
      quotes[2] != expected_quotes[2] || quotes[3] != expected_quotes[3]) {
    ++w.stats.structural_bad;
    return;
  }
  process_fields(f, w, worker, ws, we, verify_pack, bench_mode, dense_key, dense_bound,
                 dense_ready, user_dense_key, user_dense_bound, user_dense_ready, fingerprints);
}

static void scan_masked_rows(const char *p, size_t n, uint64_t abs0, uint64_t owned_end, bool eof,
                             Worker &w, unsigned worker, int64_t ws, int64_t we, bool verify_pack,
                             int bench_mode, uint64_t dense_key, uint32_t dense_bound,
                             bool dense_ready, uint64_t user_dense_key, uint32_t user_dense_bound,
                             bool user_dense_ready, bool fingerprints) {
  const __m256i comma = _mm256_set1_epi8(','), quote = _mm256_set1_epi8('"');
  const __m256i newline = _mm256_set1_epi8('\n');
  const char *line = p;
  uint64_t line_abs = abs0;
  uint64_t commas[4] = {0,0,0,0}, quotes[4] = {0,0,0,0};
  unsigned line_len = 0;
  size_t i = 0;
  while (i < n) {
    unsigned take = unsigned(std::min<size_t>(32, n - i));
    uint32_t cm = 0, qm = 0, nm = 0;
    if (take == 32) {
      __m256i v = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(p + i));
      cm = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, comma)));
      qm = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, quote)));
      nm = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, newline)));
    } else {
      for (unsigned j = 0; j < take; ++j) {
        cm |= uint32_t(p[i+j] == ',') << j;
        qm |= uint32_t(p[i+j] == '"') << j;
        nm |= uint32_t(p[i+j] == '\n') << j;
      }
    }
    unsigned base = 0;
    while (nm) {
      unsigned bit = unsigned(__builtin_ctz(nm));
      unsigned width = bit - base;
      uint32_t low = base ? ((uint32_t(1) << base) - 1) : 0;
      uint32_t high = bit == 32 ? UINT32_MAX : ((uint32_t(1) << bit) - 1);
      uint32_t segment = high & ~low;
      append_mask(commas, (cm & segment) >> base, line_len);
      append_mask(quotes, (qm & segment) >> base, line_len);
      line_len += width;
      if (line_abs < owned_end)
        consume_masked_row(line, line_len, commas, quotes, w, worker, ws, we, verify_pack,
                           bench_mode, dense_key, dense_bound, dense_ready, user_dense_key,
                           user_dense_bound, user_dense_ready, fingerprints);
      line = p + i + bit + 1;
      line_abs = abs0 + i + bit + 1;
      line_len = 0;
      std::memset(commas, 0, sizeof(commas));
      std::memset(quotes, 0, sizeof(quotes));
      base = bit + 1;
      nm &= nm - 1;
    }
    if (base < take) {
      uint32_t tail = base ? ~((uint32_t(1) << base) - 1) : UINT32_MAX;
      if (take < 32) tail &= (uint32_t(1) << take) - 1;
      append_mask(commas, (cm & tail) >> base, line_len);
      append_mask(quotes, (qm & tail) >> base, line_len);
      line_len += take - base;
    }
    i += take;
  }
  if (eof && line_len && line_abs < owned_end)
    consume_masked_row(line, line_len, commas, quotes, w, worker, ws, we, verify_pack,
                       bench_mode, dense_key, dense_bound, dense_ready, user_dense_key,
                       user_dense_bound, user_dense_ready, fingerprints);
}

static bool load_task(const Task &task, std::vector<char> &buf, Stats &stats) {
  int fd = ::open(task.path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  uint64_t wanted_end = std::min<uint64_t>(task.file_size, task.end + OVERLAP);
  size_t want = size_t(wanted_end - task.begin);
  buf.resize(want);
  size_t got = 0;
  while (got < want) {
    ssize_t n = ::pread(fd, buf.data() + got, want - got, off_t(task.begin + got));
    if (n < 0) { ::close(fd); return false; }
    if (n == 0) break;
    got += size_t(n);
  }
  buf.resize(got);
  // Grow until the row crossing the task boundary is complete. This is the
  // exact long-line fallback; ordinary generated rows never enter it.
  while (task.end < task.file_size && got &&
         std::find(buf.begin() + std::min<size_t>(buf.size(), size_t(task.end - task.begin)), buf.end(), '\n') == buf.end()) {
    size_t old = buf.size();
    size_t add = size_t(std::min<uint64_t>(OVERLAP, task.file_size - (task.begin + old)));
    if (!add) break;
    buf.resize(old + add);
    ssize_t n = ::pread(fd, buf.data() + old, add, off_t(task.begin + old));
    if (n <= 0) { buf.resize(old); break; }
    buf.resize(old + size_t(n));
    got = buf.size();
    ++stats.boundary_grows;
  }
  ::close(fd);
  return true;
}

static void process_task(const Task &task, Worker &w, unsigned worker, int64_t ws, int64_t we,
                         bool verify_pack, int bench_mode, uint64_t dense_key, uint32_t dense_bound,
                         bool dense_ready, uint64_t user_dense_key, uint32_t user_dense_bound,
                         bool user_dense_ready, bool fingerprints) {
  std::vector<char> buf;
  if (!load_task(task, buf, w.stats)) std::abort();
  size_t pos = 0;
  // Every file has a header. Nonzero chunks discard the partial first row;
  // the preceding chunk owns it because its start is below that chunk's end.
  if (task.begin == 0) {
    auto first_nl = std::find(buf.begin(), buf.end(), '\n');
    if (first_nl == buf.end()) return;
    pos = size_t(first_nl - buf.begin()) + 1;
  } else {
    char previous = 0;
    int fd = ::open(task.path.c_str(), O_RDONLY);
    if (fd < 0 || ::pread(fd, &previous, 1, off_t(task.begin - 1)) != 1) std::abort();
    ::close(fd);
    if (previous != '\n') {
      auto first_nl = std::find(buf.begin(), buf.end(), '\n');
      if (first_nl == buf.end()) return;
      pos = size_t(first_nl - buf.begin()) + 1;
    }
  }
  scan_masked_rows(buf.data() + pos, buf.size() - pos, task.begin + pos, task.end,
                   task.end == task.file_size, w, worker, ws, we, verify_pack,
                   bench_mode, dense_key, dense_bound, dense_ready, user_dense_key,
                   user_dense_bound, user_dense_ready, fingerprints);
}

struct DedupStats {
  uint64_t unique = 0;
  uint64_t valid = 0;
  uint64_t checksum = 0;
};

struct SessionEvent { uint32_t ts_rel; uint32_t pc; };
static_assert(sizeof(SessionEvent)==8);
struct SessionSource { uint32_t user_index,ts_rel,meta,location; };
static_assert(sizeof(SessionSource)==16);
struct CompactSession { uint32_t person, start; uint64_t revenue; };
struct SessionKernelStats { uint64_t people=0,sessions=0,events=0,revenue=0; uint64_t checksum=0; double catalog_s=0,prep_s=0,count_s=0,scatter_s=0,kernel_s=0,finalize_s=0; };
static std::unordered_map<uint64_t,uint32_t> g_dense_extra_codes;
static std::unordered_map<std::string,uint32_t> g_generic_extra_codes;
static std::vector<uint64_t> g_dense_extra_values;
static std::vector<std::string> g_generic_extra_values;
static uint32_t g_linked_base=0,g_person_codes=0;
struct ProductCatalog {
  std::vector<std::string> categories;
  std::unordered_map<uint64_t,uint8_t> packed;
  std::unordered_map<std::string,uint8_t> generic;
  uint8_t unknown=0;
};
static ProductCatalog load_products(fs::path const&data) {
  ProductCatalog c;std::unordered_map<std::string,uint8_t> cats;std::ifstream in(data/"products.csv",std::ios::binary);std::string line;std::getline(in,line);
  while(std::getline(in,line)){std::string_view p,cat;if(!IdentityMap::split2(line,p,cat))continue;auto[it,newcat]=cats.emplace(std::string(cat),uint8_t(cats.size()));if(newcat)c.categories.emplace_back(cat);uint64_t key=0;if(parse_packed(p,'p',key))c.packed.emplace(key,it->second);else c.generic.emplace(std::string(p),it->second);}
  c.unknown=uint8_t(c.categories.size());c.categories.emplace_back("unknown");return c;
}
struct UserDailyTemp{uint32_t person;uint16_t day;uint16_t top_category;uint32_t sessions,events;uint64_t revenue;};

static SessionKernelStats build_sessions(fs::path const&data,std::vector<Worker>&workers, IdentityMap const&id,
                                         uint32_t person_bound, int ncpu, int64_t ws, int64_t we,
                                         uint64_t user_key, bool fingerprints,
                                         std::vector<CompactSession>* sessions_out=nullptr,
                                         std::vector<uint32_t>* session_offsets_out=nullptr,
                                         std::vector<UserDailyTemp>* daily_out=nullptr,
                                         std::vector<SessionSource> const* session_sources=nullptr) {
  auto catalog_started=Clock::now();ProductCatalog products=load_products(data);double catalog_s=std::chrono::duration<double>(Clock::now()-catalog_started).count();auto prep_started=Clock::now();std::vector<std::vector<std::pair<uint32_t,uint8_t>>> product_cat(workers.size());
  for(size_t wi=0;wi<workers.size();++wi){auto&dst=product_cat[wi];dst.reserve(workers[wi].products.size());for(auto const&p:workers[wi].products){auto it=products.packed.find(p.product_key);dst.emplace_back(p.location,it==products.packed.end()?products.unknown:it->second);}std::sort(dst.begin(),dst.end());}
  const uint32_t linked_base=uint32_t(id.names.size());
  const uint64_t ordinary_codes=uint64_t(person_bound)*9;
  std::vector<uint64_t> dense_extras;std::vector<std::string> generic_extras;
  auto selected=[&](Worker const&w,GenericEvent const&g){if(g.generic_id)return g.winner&&g.semantic_valid;if(g.location==UINT32_MAX)return false;unsigned wi=g.location>>29,part=(g.location>>23)&63;uint32_t ix=g.location&((1u<<23)-1);return wi<workers.size()&&ix<workers[wi].events[part].size()&&(workers[wi].events[part][ix].meta&(1u<<29))&&g.semantic_valid;};
  for(auto const&w:workers)for(uint32_t loc:w.high_alias_locations){unsigned wi=loc>>29,part=(loc>>23)&63;uint32_t ix=loc&((1u<<23)-1);if(wi>=workers.size()||ix>=workers[wi].events[part].size())continue;auto const&e=workers[wi].events[part][ix];if((e.meta&(1u<<29))&&(e.meta&(1u<<31))&&!(e.meta&(1u<<30))&&id.resolve_dense(e.user_index)==UINT32_MAX)dense_extras.push_back(e.user_index);}
  for(auto const&w:workers)for(auto const&g:w.generic)if(selected(w,g)){uint64_t p=0;if(parse_packed(g.fields[1],'u',p)){uint64_t q=decode_number(packed_number(p),user_key);if(id.resolve_dense(q)==UINT32_MAX&&!((q<uint64_t(person_bound)*512)&&((q&511)<9)))dense_extras.push_back(q);}else if(id.generic_nodes.find(g.fields[1])==id.generic_nodes.end())generic_extras.push_back(g.fields[1]);}
  std::sort(dense_extras.begin(),dense_extras.end());dense_extras.erase(std::unique(dense_extras.begin(),dense_extras.end()),dense_extras.end());std::sort(generic_extras.begin(),generic_extras.end());generic_extras.erase(std::unique(generic_extras.begin(),generic_extras.end()),generic_extras.end());
  uint64_t extra_base64=uint64_t(linked_base)+ordinary_codes;if(extra_base64+dense_extras.size()+generic_extras.size()>=UINT32_MAX)return{};uint32_t extra_base=uint32_t(extra_base64),person_codes=uint32_t(extra_base64+dense_extras.size()+generic_extras.size());
  std::unordered_map<uint64_t,uint32_t> dense_extra_codes;dense_extra_codes.reserve(dense_extras.size()*2+1);for(size_t i=0;i<dense_extras.size();++i)dense_extra_codes.emplace(dense_extras[i],extra_base+uint32_t(i));
  std::unordered_map<std::string,uint32_t> generic_extra_codes;generic_extra_codes.reserve(generic_extras.size()*2+1);for(size_t i=0;i<generic_extras.size();++i)generic_extra_codes.emplace(generic_extras[i],extra_base+uint32_t(dense_extras.size()+i));
  g_dense_extra_codes=dense_extra_codes;g_generic_extra_codes=generic_extra_codes;g_linked_base=linked_base;g_person_codes=person_codes;
  g_dense_extra_values=dense_extras;g_generic_extra_values=generic_extras;
  auto code_for=[&](uint64_t q)->uint32_t{
    uint32_t linked=id.resolve_dense(q); if(linked!=UINT32_MAX)return linked;
    uint64_t p=q>>9,a=q&511;
    if(p<person_bound&&a<9)return linked_base+uint32_t(p*9+a);
    auto it=dense_extra_codes.find(q);return it==dense_extra_codes.end()?UINT32_MAX:it->second;
  };
  auto code_for_generic=[&](std::string const&s){auto it=id.generic_nodes.find(s);if(it!=id.generic_nodes.end())return id.parent[it->second];auto jt=generic_extra_codes.find(s);return jt==generic_extra_codes.end()?UINT32_MAX:jt->second;};
  struct Side{uint32_t code;SessionEvent event;};std::vector<Side>sides;sides.reserve(50'000);
  for(auto const&w:workers)for(auto const&g:w.generic)if(selected(w,g)){uint32_t code=UINT32_MAX;uint64_t p=0;if(parse_packed(g.fields[1],'u',p))code=code_for(decode_number(packed_number(p),user_key));else code=code_for_generic(g.fields[1]);int64_t ts=0;uint32_t price=0,pc=0;if(code!=UINT32_MAX&&parse_ts(g.fields[2],ts)){if(event_type(g.fields[4])==2){if(!parse_price(g.fields[6],price))continue;uint8_t cat=products.unknown;uint64_t pk=0;if(parse_packed(g.fields[5],'p',pk)){auto it=products.packed.find(pk);if(it!=products.packed.end())cat=it->second;}else{auto it=products.generic.find(g.fields[5]);if(it!=products.generic.end())cat=it->second;}pc=price|(uint32_t(cat)<<24);}sides.push_back({code,{uint32_t(ts-ws),pc}});}}

  // Generated inputs have a useful exact ordering property: dense event-ID
  // order contains one dominant, chronological run for each original user.
  // Exploit it only after proving the property over the complete winner stream;
  // all revised users, cross-original link components, generic rows, and any
  // non-monotonic target are diverted to the exact occupancy fallback.
  if(session_sources && !session_sources->empty()) {
    struct OrderedItem { uint32_t orig,code; SessionEvent event; };
    std::array<std::vector<OrderedItem>,8> item_local;std::vector<std::thread> item_threads;
    for(int tid=0;tid<ncpu;++tid)item_threads.emplace_back([&,tid]{size_t lo=session_sources->size()*tid/ncpu,hi=session_sources->size()*(tid+1)/ncpu;auto&dst=item_local[tid];dst.reserve(hi-lo);
      for(size_t di=lo;di<hi;++di){auto const&s=(*session_sources)[di];uint32_t code=code_for(s.user_index);if(code==UINT32_MAX)continue;unsigned wi=s.location>>29;
        uint32_t pc=0;if(((s.meta>>24)&3)==2){auto const&v=product_cat[wi];auto it=std::lower_bound(v.begin(),v.end(),std::pair<uint32_t,uint8_t>{s.location,0});uint8_t cat=it!=v.end()&&it->first==s.location?it->second:products.unknown;pc=(s.meta&0xffffffu)|(uint32_t(cat)<<24);}
        dst.push_back({s.user_index>>9,code,{s.ts_rel,pc}});
      }});
    for(auto&t:item_threads)t.join();size_t item_count=0;for(int t=0;t<ncpu;++t)item_count+=item_local[t].size();std::vector<OrderedItem> items;items.reserve(item_count);for(int t=0;t<ncpu;++t)items.insert(items.end(),item_local[t].begin(),item_local[t].end());
    struct Run{uint32_t orig;size_t lo,hi;};std::vector<Run> runs;runs.reserve(person_bound+1024);
    uint32_t max_orig=0;for(size_t i=0;i<items.size();){size_t j=i+1;while(j<items.size()&&items[j].orig==items[i].orig)++j;runs.push_back({items[i].orig,i,j});max_orig=std::max(max_orig,items[i].orig);i=j;}
    uint32_t observed=max_orig+1;std::vector<size_t> best(observed,SIZE_MAX);
    for(size_t i=0;i<runs.size();++i)if(runs[i].orig<observed&&(best[runs[i].orig]==SIZE_MAX||runs[i].hi-runs[i].lo>runs[best[runs[i].orig]].hi-runs[best[runs[i].orig]].lo))best[runs[i].orig]=i;
    std::vector<size_t> starts(observed);bool guard=observed&&observed<=person_bound;
    for(uint32_t p=0;guard&&p<observed;++p){if(best[p]==SIZE_MAX){guard=false;break;}starts[p]=runs[best[p]].lo;if(p&&starts[p]<=starts[p-1])guard=false;}
    if(guard) {
      std::vector<uint32_t> rmin(id.names.size(),UINT32_MAX),rmax(id.names.size());
      for(uint32_t node=0;node<id.names.size();++node){uint64_t q=id.names[node].dense;if(q==UINT64_MAX)continue;uint32_t r=id.parent[node],p=uint32_t(q>>9);rmin[r]=std::min(rmin[r],p);rmax[r]=std::max(rmax[r],p);}
      std::vector<uint8_t> exceptional(person_codes);
      for(uint32_t r=0;r<rmin.size()&&r<exceptional.size();++r)if(rmin[r]!=UINT32_MAX&&rmin[r]!=rmax[r])exceptional[r]=1;
      std::vector<uint32_t> last(person_codes),seen(person_codes);uint32_t gp=0;
      for(size_t i=0;i<items.size();++i){while(gp+1<observed&&i>=starts[gp+1])++gp;auto const&x=items[i];if(x.orig!=gp)exceptional[x.code]=1;if(seen[x.code]&&x.event.ts_rel<last[x.code])exceptional[x.code]=1;seen[x.code]=1;last[x.code]=x.event.ts_rel;}
      for(auto const&s:sides)exceptional[s.code]=1;
      struct CE{uint32_t code;SessionEvent event;};std::array<std::vector<CE>,8> exlocal;std::array<std::vector<CompactSession>,8> slocal;std::array<std::vector<UserDailyTemp>,8> dlocal;std::array<SessionKernelStats,8> zlocal;
      unsigned ndays=unsigned((we-ws+DAY_MS-1)/DAY_MS),nc=products.categories.size();constexpr uint32_t gap=1'800'000;
      auto consume_sorted=[&](uint32_t code,std::vector<SessionEvent>const&v,int tid){if(v.empty())return;auto&z=zlocal[tid];++z.people;std::vector<uint32_t>ds(ndays),de(ndays);std::vector<uint64_t>dr(ndays),cats(size_t(ndays)*nc);bool active=false;uint32_t prev=0,start=0;uint64_t sr=0;unsigned day=0;
        for(auto e:v){if(!active||uint64_t(e.ts_rel)-prev>gap){if(active){z.revenue+=sr;slocal[tid].push_back({code,start,sr});}active=true;start=e.ts_rel;day=start/DAY_MS;sr=0;++z.sessions;++ds[day];if(fingerprints)z.checksum^=mix64(uint64_t(code)^(uint64_t(start)<<20));}prev=e.ts_rel;++z.events;++de[day];uint32_t price=e.pc&0xffffffu;z.revenue+=0;dr[day]+=price;sr+=price;if(price)cats[size_t(day)*nc+(e.pc>>24)]+=price;}
        if(active){z.revenue+=sr;slocal[tid].push_back({code,start,sr});}for(unsigned d=0;d<ndays;++d)if(ds[d]){uint16_t top=UINT16_MAX;if(dr[d]){uint64_t bv=0;for(unsigned c=0;c<nc;++c){uint64_t q=cats[size_t(d)*nc+c];if(q>bv||(q==bv&&q&&(top==UINT16_MAX||products.categories[c]<products.categories[top]))){bv=q;top=c;}}}dlocal[tid].push_back({code,uint16_t(d),top,ds[d],de[d],dr[d]});}};
      auto phase=Clock::now();std::vector<std::thread> ts;
      for(int tid=0;tid<ncpu;++tid)ts.emplace_back([&,tid]{uint32_t p0=uint64_t(observed)*tid/ncpu,p1=uint64_t(observed)*(tid+1)/ncpu;size_t at=starts[p0],endall=p1<observed?starts[p1]:items.size();uint32_t p=p0;
        while(at<endall){while(p+1<p1&&at>=starts[p+1])++p;size_t end=p+1<p1?starts[p+1]:endall;std::vector<std::pair<uint32_t,std::vector<SessionEvent>>> local;
          for(size_t i=at;i<end;++i){auto const&x=items[i];if(exceptional[x.code]){exlocal[tid].push_back({x.code,x.event});continue;}auto it=std::find_if(local.begin(),local.end(),[&](auto const&q){return q.first==x.code;});if(it==local.end()){local.push_back({x.code,{}});it=local.end()-1;}it->second.push_back(x.event);}
          for(auto const&q:local)consume_sorted(q.first,q.second,tid);at=end;}
      });for(auto&t:ts)t.join();ts.clear();
      std::vector<uint32_t> ecodes,eindex(person_codes,UINT32_MAX);for(uint32_t c=0;c<person_codes;++c)if(exceptional[c]){eindex[c]=ecodes.size();ecodes.push_back(c);}std::vector<std::vector<SessionEvent>> evs(ecodes.size());
      for(auto const&v:exlocal)for(auto const&e:v)evs[eindex[e.code]].push_back(e.event);for(auto const&s:sides)evs[eindex[s.code]].push_back(s.event);
      std::atomic<size_t> en{0};for(int tid=0;tid<ncpu;++tid)ts.emplace_back([&,tid]{size_t nb=(we-ws+gap-1)/gap,nw=(nb+63)/64;while(true){size_t ei=en.fetch_add(1);if(ei>=evs.size())break;auto const&v=evs[ei];if(v.empty())continue;uint32_t code=ecodes[ei];auto&z=zlocal[tid];++z.people;std::vector<uint32_t>mn(nb,UINT32_MAX),mx(nb),cnt(nb),ds(ndays),de(ndays);std::vector<uint64_t>rev(nb),bits(nw),dr(ndays),cats(size_t(ndays)*nc);std::vector<uint16_t>bd(nb);
          for(auto e:v){size_t b=e.ts_rel/gap;mn[b]=std::min(mn[b],e.ts_rel);mx[b]=std::max(mx[b],e.ts_rel);++cnt[b];rev[b]+=e.pc&0xffffffu;bits[b>>6]|=1ULL<<(b&63);}bool active=false;uint32_t prev=0,start=0;uint64_t sr=0;unsigned day=0;
          for(size_t wi=0;wi<nw;++wi){uint64_t w=bits[wi];while(w){unsigned bit=__builtin_ctzll(w);w&=w-1;size_t b=wi*64+bit;if(!active||uint64_t(mn[b])-prev>gap){if(active){z.revenue+=sr;slocal[tid].push_back({code,start,sr});}active=true;start=mn[b];day=start/DAY_MS;sr=0;++z.sessions;++ds[day];if(fingerprints)z.checksum^=mix64(uint64_t(code)^(uint64_t(start)<<20));}bd[b]=day;prev=mx[b];z.events+=cnt[b];de[day]+=cnt[b];dr[day]+=rev[b];sr+=rev[b];}}
          if(active){z.revenue+=sr;slocal[tid].push_back({code,start,sr});}for(auto e:v){uint32_t price=e.pc&0xffffffu;if(price)cats[size_t(bd[e.ts_rel/gap])*nc+(e.pc>>24)]+=price;}for(unsigned d=0;d<ndays;++d)if(ds[d]){uint16_t top=UINT16_MAX;if(dr[d]){uint64_t bv=0;for(unsigned c=0;c<nc;++c){uint64_t q=cats[size_t(d)*nc+c];if(q>bv||(q==bv&&q&&(top==UINT16_MAX||products.categories[c]<products.categories[top]))){bv=q;top=c;}}}dlocal[tid].push_back({code,uint16_t(d),top,ds[d],de[d],dr[d]});}
        }});
      for(auto&t:ts)t.join();SessionKernelStats out;out.kernel_s=std::chrono::duration<double>(Clock::now()-phase).count();for(int t=0;t<ncpu;++t){out.people+=zlocal[t].people;out.sessions+=zlocal[t].sessions;out.events+=zlocal[t].events;out.revenue+=zlocal[t].revenue;out.checksum^=zlocal[t].checksum;}
      if(sessions_out){std::vector<uint32_t>cnt(person_codes),off(size_t(person_codes)+1);for(int t=0;t<ncpu;++t)for(auto const&s:slocal[t])++cnt[s.person];uint32_t n=0;for(uint32_t c=0;c<person_codes;++c){off[c]=n;n+=cnt[c];cnt[c]=off[c];}off[person_codes]=n;sessions_out->resize(n);for(int t=0;t<ncpu;++t)for(auto const&s:slocal[t])(*sessions_out)[cnt[s.person]++]=s;if(session_offsets_out)*session_offsets_out=std::move(off);}
      if(daily_out)for(int t=0;t<ncpu;++t)daily_out->insert(daily_out->end(),dlocal[t].begin(),dlocal[t].end());
      return out;
    }
  }
  double prep_s=std::chrono::duration<double>(Clock::now()-prep_started).count();auto phase=Clock::now();std::vector<uint32_t> counts(person_codes);
  std::vector<std::thread> ts;
  for(int tid=0;tid<ncpu;++tid)ts.emplace_back([&,tid]{
    for(unsigned part=tid;part<PARTITIONS;part+=ncpu)for(size_t wi=0;wi<workers.size();++wi)
      for(auto&e:workers[wi].events[part])if((e.meta&(1u<<29))&&(e.meta&(1u<<31))&&!(e.meta&(1u<<30))){uint32_t c=code_for(e.user_index);if(c!=UINT32_MAX){e.user_index=c;e.meta|=1u<<28;if(c<linked_base)std::atomic_ref<uint32_t>(counts[c]).fetch_add(1,std::memory_order_relaxed);else ++counts[c];}}
  });
  for(auto&t:ts)t.join();ts.clear();for(auto const&s:sides)++counts[s.code];double count_s=std::chrono::duration<double>(Clock::now()-phase).count();phase=Clock::now();
  std::vector<uint32_t> offsets(size_t(person_codes)+1),active_people;active_people.reserve(person_bound);uint64_t total=0;
  for(uint32_t i=0;i<person_codes;++i){offsets[i]=uint32_t(total);uint32_t c=counts[i];total+=c;if(c)active_people.push_back(i);}
  offsets[person_codes]=uint32_t(total);
  std::vector<uint32_t> cursor(offsets.begin(),offsets.end()-1);
  std::vector<SessionEvent> grouped(total);
  for(int tid=0;tid<ncpu;++tid)ts.emplace_back([&,tid]{
    for(unsigned part=tid;part<PARTITIONS;part+=ncpu)for(size_t wi=0;wi<workers.size();++wi){auto const&pcats=product_cat[wi];uint32_t loc0=encode_location(unsigned(wi),part,0);size_t pci=std::lower_bound(pcats.begin(),pcats.end(),std::pair<uint32_t,uint8_t>{loc0,0})-pcats.begin();
      for(uint32_t ix=0;ix<workers[wi].events[part].size();++ix){auto const&e=workers[wi].events[part][ix];if((e.meta&(1u<<28))&&(e.meta&(1u<<29))&&(e.meta&(1u<<31))&&!(e.meta&(1u<<30))){uint32_t c=e.user_index;uint32_t at=c<linked_base?std::atomic_ref<uint32_t>(cursor[c]).fetch_add(1,std::memory_order_relaxed):cursor[c]++;uint32_t pc=0;if(((e.meta>>24)&3)==2){uint32_t loc=encode_location(unsigned(wi),part,ix);while(pci<pcats.size()&&pcats[pci].first<loc)++pci;uint8_t cat=pci<pcats.size()&&pcats[pci].first==loc?pcats[pci].second:products.unknown;pc=(e.meta&0xffffffu)|(uint32_t(cat)<<24);}grouped[at]={e.ts_rel,pc};}}}
  });
  for(auto&t:ts)t.join();ts.clear();for(auto const&s:sides){uint32_t at=cursor[s.code]++;grouped[at]=s.event;}double scatter_s=std::chrono::duration<double>(Clock::now()-phase).count();phase=Clock::now();

  const uint32_t gap=1'800'000;
  const size_t nbins=size_t((we-ws+gap-1)/gap),nwords=(nbins+63)/64;
  std::array<size_t,9> active_cut{};for(int t=0;t<=ncpu;++t){uint64_t target=total*uint64_t(t)/ncpu;active_cut[t]=std::lower_bound(active_people.begin(),active_people.end(),target,[&](uint32_t p,uint64_t x){return uint64_t(offsets[p])<x;})-active_people.begin();}active_cut[0]=0;active_cut[ncpu]=active_people.size();
  std::vector<uint32_t> person_session_counts(sessions_out?person_codes:0);std::array<SessionKernelStats,8> stats{};std::array<std::vector<CompactSession>,8> session_local;std::array<std::vector<UserDailyTemp>,8> daily_local;
  if(sessions_out)for(int t=0;t<ncpu;++t)session_local[t].reserve(grouped.size()/std::max(1,ncpu*5));
  for(int tid=0;tid<ncpu;++tid)ts.emplace_back([&,tid]{
    unsigned ndays=unsigned((we-ws+DAY_MS-1)/DAY_MS);std::vector<uint32_t> mn(nbins,UINT32_MAX),mx(nbins),cnt(nbins);std::vector<uint64_t> rev(nbins),bits(nwords);std::vector<uint16_t> bucket_day(nbins),touched;touched.reserve(128);std::vector<uint32_t> ds(ndays),de(ndays);std::vector<uint64_t> dr(ndays),cats(size_t(ndays)*products.categories.size());auto&z=stats[tid];
    auto&session_dst=session_local[tid];auto&daily_dst=daily_local[tid];for(size_t ai=active_cut[tid];ai<active_cut[tid+1];++ai){uint32_t p=active_people[ai];uint32_t lo=offsets[p],hi=offsets[p+1],sessions_before=z.sessions;++z.people;touched.clear();
      for(uint32_t j=lo;j<hi;++j){auto e=grouped[j];size_t b=e.ts_rel/gap;if(b>=nbins)continue;uint64_t bit=1ULL<<(b&63);if(!(bits[b>>6]&bit)){bits[b>>6]|=bit;touched.push_back(uint16_t(b));}mn[b]=std::min(mn[b],e.ts_rel);mx[b]=std::max(mx[b],e.ts_rel);++cnt[b];rev[b]+=e.pc&0xffffffu;}
      bool active=false;uint32_t prev=0,start=0;uint64_t session_rev=0;unsigned active_day=0;
      for(size_t wi=0;wi<nwords;++wi){uint64_t w=bits[wi];while(w){unsigned bit=__builtin_ctzll(w);w&=w-1;size_t b=wi*64+bit;if(b>=nbins)break;if(!active||uint64_t(mn[b])-prev>gap){if(active){z.revenue+=session_rev;if(sessions_out)session_dst.push_back({p,start,session_rev});}active=true;start=mn[b];active_day=unsigned(start/DAY_MS);session_rev=0;++z.sessions;++ds[active_day];if(fingerprints)z.checksum^=mix64(uint64_t(p)^(uint64_t(start)<<20));}bucket_day[b]=uint16_t(active_day);prev=mx[b];z.events+=cnt[b];de[active_day]+=cnt[b];dr[active_day]+=rev[b];session_rev+=rev[b];}}
      if(active){z.revenue+=session_rev;if(sessions_out)session_dst.push_back({p,start,session_rev});}
      for(uint32_t j=lo;j<hi;++j){uint32_t price=grouped[j].pc&0xffffffu;if(price){size_t b=grouped[j].ts_rel/gap;unsigned cat=grouped[j].pc>>24;cats[size_t(bucket_day[b])*products.categories.size()+cat]+=price;}}
      if(daily_out)for(unsigned d=0;d<ndays;++d)if(ds[d]){uint16_t top=UINT16_MAX;if(dr[d]){uint64_t best=0;for(unsigned c=0;c<products.categories.size();++c){uint64_t v=cats[size_t(d)*products.categories.size()+c];if(v>best||(v==best&&v&&products.categories[c]<products.categories[top])){best=v;top=uint16_t(c);}}}daily_dst.push_back({p,uint16_t(d),top,ds[d],de[d],dr[d]});}
      std::fill(ds.begin(),ds.end(),0);std::fill(de.begin(),de.end(),0);std::fill(dr.begin(),dr.end(),0);std::fill(cats.begin(),cats.end(),0);
      for(uint16_t b:touched){mn[b]=UINT32_MAX;mx[b]=cnt[b]=0;rev[b]=0;bits[b>>6]&=~(1ULL<<(b&63));}
      if(sessions_out)person_session_counts[p]=uint32_t(z.sessions-sessions_before);
    }
  });
  for(auto&t:ts)t.join();SessionKernelStats out;for(int i=0;i<ncpu;++i){out.people+=stats[i].people;out.sessions+=stats[i].sessions;out.events+=stats[i].events;out.revenue+=stats[i].revenue;out.checksum^=stats[i].checksum;}out.catalog_s=catalog_s;out.prep_s=prep_s;out.count_s=count_s;out.scatter_s=scatter_s;out.kernel_s=std::chrono::duration<double>(Clock::now()-phase).count();
  auto finalize_started=Clock::now();if(sessions_out){std::vector<uint32_t> so(size_t(person_codes)+1);uint32_t st=0;for(uint32_t p=0;p<person_codes;++p){so[p]=st;st+=person_session_counts[p];}so[person_codes]=st;sessions_out->clear();sessions_out->reserve(st);for(int t=0;t<ncpu;++t)sessions_out->insert(sessions_out->end(),std::make_move_iterator(session_local[t].begin()),std::make_move_iterator(session_local[t].end()));if(session_offsets_out)*session_offsets_out=std::move(so);}
  if(daily_out){size_t n=0;for(int t=0;t<ncpu;++t)n+=daily_local[t].size();daily_out->reserve(n);for(int t=0;t<ncpu;++t)daily_out->insert(daily_out->end(),std::make_move_iterator(daily_local[t].begin()),std::make_move_iterator(daily_local[t].end()));}
  out.finalize_s=std::chrono::duration<double>(Clock::now()-finalize_started).count();
  return out;
}

static DedupStats reduce_dense_slots(std::vector<Worker> &workers, uint32_t bound, int ncpu,
                                     bool fingerprints,
                                     std::vector<SessionSource>* session_sources=nullptr) {
  struct __attribute__((packed)) WinnerSlot{uint64_t ingest;uint32_t location;};
  std::array<DedupStats, DEDUP_OWNERS> owner_stats{};
  std::array<std::vector<SessionSource>,DEDUP_OWNERS> owner_sources;
  std::atomic<unsigned> next_owner{0};
  std::vector<std::thread> ts;
  for (int tid = 0; tid < ncpu; ++tid) {
    ts.emplace_back([&] {
      while (true) {
        unsigned owner = next_owner.fetch_add(1, std::memory_order_relaxed);
        if (owner >= DEDUP_OWNERS) break;
        size_t lo = (size_t(owner) * bound + DEDUP_OWNERS - 1) / DEDUP_OWNERS;
        size_t hi = (size_t(owner + 1) * bound + DEDUP_OWNERS - 1) / DEDUP_OWNERS;
        std::vector<WinnerSlot> slots(hi-lo,WinnerSlot{UINT64_MAX,UINT32_MAX});
        if(session_sources) owner_sources[owner].reserve((hi-lo)*9/10);
        for (auto const &w : workers) {
          for (auto const &aux : w.dedup[owner]) {
            size_t si = size_t(aux.dense_index) - lo;
            WinnerSlot &slot = slots[si];
            uint64_t ingest = aux_ingest(aux);
            if (ingest < slot.ingest || (ingest == slot.ingest && aux.location < slot.location)) {
              slot.ingest = ingest;
              slot.location = aux.location;
            }
          }
        }
        DedupStats z;
        for (size_t j = 0; j < slots.size(); ++j) {
          if (slots[j].ingest == UINT64_MAX) continue;
          uint32_t loc = slots[j].location;
          unsigned worker = loc >> 29;
          unsigned part = (loc >> 23) & 63;
          uint32_t index = loc & ((1u << 23) - 1);
          CompactEvent &ev = workers[worker].events[part][index];
          ++z.unique;
          ev.meta |= 1u << 29; // winner marker, consumed in place by person scatter
          if (ev.meta & (1u << 31)) ++z.valid;
          if(session_sources&&(ev.meta&(1u<<31))&&!(ev.meta&(1u<<30)))
            owner_sources[owner].push_back({ev.user_index,ev.ts_rel,ev.meta,loc});
          if (fingerprints)
            z.checksum ^= mix64(uint64_t(lo + j) ^ ev.user_index ^ (uint64_t(ev.ts_rel) << 17) ^
                                (uint64_t(ev.meta) << 32));
        }
        owner_stats[owner] = z;
      }
    });
  }
  for (auto &t : ts) t.join();
  DedupStats out;
  for (auto const &z : owner_stats) {
    out.unique += z.unique;
    out.valid += z.valid;
    out.checksum ^= z.checksum;
  }
  if(session_sources){size_t n=0;for(auto const&v:owner_sources)n+=v.size();session_sources->reserve(n);for(auto&v:owner_sources)session_sources->insert(session_sources->end(),std::make_move_iterator(v.begin()),std::make_move_iterator(v.end()));}

  struct GenericChoice { std::string id; uint64_t ingest; GenericEvent *row; };
  std::vector<GenericChoice> generic;
  for (auto &w : workers) for (auto &g : w.generic) if (g.generic_id) {
    int64_t ingest = 0;
    if (parse_calendar(g.fields[3], ingest))
      generic.push_back(GenericChoice{g.fields[0], uint64_t(ingest) ^ (1ULL << 63), &g});
  }
  std::sort(generic.begin(), generic.end(), [](auto const &a, auto const &b) {
    return a.id < b.id || (a.id == b.id && a.ingest < b.ingest);
  });
  for (size_t i = 0; i < generic.size();) {
    size_t j = i + 1;
    while (j < generic.size() && generic[j].id == generic[i].id) ++j;
    ++out.unique;
    generic[i].row->winner = true;
    if (generic[i].row->semantic_valid) ++out.valid;
    i = j;
  }
  return out;
}

struct FastClick {
  uint32_t person, ts;
  uint16_t campaign, generic;
  uint64_t id;
};
struct ClickResult {
  uint64_t physical=0, structural_bad=0, empty=0, invalid=0, retained=0;
  uint64_t credited=0, assisted=0, revenue=0;
  double scan_seconds=0, tail_seconds=0;
  double catalog_seconds=0,group_seconds=0, sort_seconds=0, sweep_seconds=0, reduce_seconds=0;
};
struct CampaignDailyTemp{uint16_t campaign,day;uint32_t sessions,purchasing,assists;uint64_t revenue;};
struct Campaigns {
  std::vector<std::string> ids, channels;
  std::unordered_map<std::string,uint16_t> exact;
  std::vector<int16_t> numeric;
};
static Campaigns read_campaigns(fs::path const&data) {
  Campaigns c; std::ifstream in(data/"campaigns.csv",std::ios::binary); std::string line;
  std::getline(in,line);
  while(std::getline(in,line)) {
    std::string_view a,b; if(!IdentityMap::split2(line,a,b)||a.empty()) continue;
    uint16_t ix=uint16_t(c.ids.size()); c.ids.emplace_back(a); c.channels.emplace_back(b); c.exact.emplace(c.ids.back(),ix);
    if(a.size()>2&&a[0]=='c'&&a[1]=='_') { uint64_t n=0; bool ok=true; for(size_t i=2;i<a.size();++i){if(a[i]<'0'||a[i]>'9'){ok=false;break;}n=n*10+a[i]-'0';}
      if(ok&&n<1'000'000){if(c.numeric.size()<=n)c.numeric.resize(n+1,-1);c.numeric[n]=int16_t(ix);} }
  }
  return c;
}
static int campaign_id(Campaigns const&c,std::string_view s) {
  if(s.size()>2&&s[0]=='c'&&s[1]=='_'){uint64_t n=0;bool ok=true;for(size_t i=2;i<s.size();++i){if(s[i]<'0'||s[i]>'9'){ok=false;break;}n=n*10+s[i]-'0';}if(ok&&n<c.numeric.size()&&c.numeric[n]>=0)return c.numeric[n];}
  auto it=c.exact.find(std::string(s)); return it==c.exact.end()?-1:it->second;
}
static bool fields4(std::string_view line,std::array<std::string_view,4>&f) {
  if(line.empty()) return false; size_t s=0;
  for(unsigned i=0;i<4;++i){size_t e=i==3?line.size():line.find(',',s);if(e==line.npos)return false;auto x=line.substr(s,e-s);
    if(i==3&&x.find(',')!=x.npos)return false;
    if(!x.empty()&&x.front()=='"'){if(x.size()<2||x.back()!='"')return false;x.remove_prefix(1);x.remove_suffix(1);if(x.find('"')!=x.npos)return false;}else if(x.find('"')!=x.npos)return false;f[i]=x;s=e+1;}
  return s==line.size()+1;
}
static ClickResult run_clicks(fs::path const&data,int ncpu,int64_t ws,int64_t we,uint64_t user_key,
                              uint32_t person_bound,IdentityMap const&id,
                              std::vector<CompactSession>const&sessions,
                              std::vector<uint32_t>const&session_offsets,
                              std::vector<CampaignDailyTemp>* campaign_out=nullptr) {
  auto begun=Clock::now(); ClickResult out; Campaigns campaigns=read_campaigns(data);out.catalog_seconds=std::chrono::duration<double>(Clock::now()-begun).count();
#ifndef CLICK_BUCKETS_CFG
#define CLICK_BUCKETS_CFG 256
#endif
  static constexpr uint32_t CLICK_BUCKETS = CLICK_BUCKETS_CFG;
  struct CW { std::array<std::vector<FastClick>,CLICK_BUCKETS> rows; std::vector<std::string> generic_ids; ClickResult stats; Stats io; };
  std::vector<CW> workers(ncpu); std::vector<Task> tasks;
  for(auto const&e:fs::directory_iterator(data/"clicks")) if(e.is_regular_file()&&e.path().extension()==".csv") {
    uint64_t n=e.file_size(); for(uint64_t b=0;b<n;b+=CHUNK) tasks.push_back({e.path(),n,b,std::min<uint64_t>(n,b+CHUNK)});
  }
  auto dense_code=[&](uint64_t q)->uint32_t { uint32_t root=id.resolve_dense(q); if(root!=UINT32_MAX)return root;
    uint64_t p=q>>9,a=q&511; if(p<person_bound&&a<9)return g_linked_base+uint32_t(p*9+a);
    auto it=g_dense_extra_codes.find(q);return it==g_dense_extra_codes.end()?UINT32_MAX:it->second; };
  std::atomic<size_t> next{0}; std::vector<std::thread> threads;
  uint32_t person_stride=(g_person_codes+CLICK_BUCKETS-1)/CLICK_BUCKETS;
  for(int tid=0;tid<ncpu;++tid) threads.emplace_back([&,tid]{
    CW& w=workers[tid]; for(auto&v:w.rows)v.reserve((800'000+CLICK_BUCKETS-1)/CLICK_BUCKETS); DateCache cache;
    while(true){size_t ti=next.fetch_add(1);if(ti>=tasks.size())break;std::vector<char>buf;if(!load_task(tasks[ti],buf,w.io))std::abort();size_t pos=0;
      if(tasks[ti].begin==0){auto p=std::find(buf.begin(),buf.end(),'\n');if(p==buf.end())continue;pos=size_t(p-buf.begin())+1;}
      else{char prev=0;int fd=open(tasks[ti].path.c_str(),O_RDONLY);pread(fd,&prev,1,tasks[ti].begin-1);close(fd);if(prev!='\n'){auto p=std::find(buf.begin(),buf.end(),'\n');if(p==buf.end())continue;pos=size_t(p-buf.begin())+1;}}
      while(pos<buf.size()){size_t e=pos;while(e<buf.size()&&buf[e]!='\n')++e;if(tasks[ti].begin+pos>=tasks[ti].end)break;if(e==buf.size()&&tasks[ti].end<tasks[ti].file_size)break;++w.stats.physical;
        std::array<std::string_view,4>f;if(!fields4({buf.data()+pos,e-pos},f)){++w.stats.structural_bad;pos=e+1;continue;}if(f[0].empty()){++w.stats.empty;pos=e+1;continue;}
        int64_t tm=0;int camp=campaign_id(campaigns,f[2]);if(f[1].empty()||!fast_ts(f[3],cache,tm)||tm<ws-int64_t(7*DAY_MS)||tm>=we||camp<0){++w.stats.invalid;pos=e+1;continue;}
        uint32_t person=UINT32_MAX;uint64_t up=0;if(parse_packed(f[1],'u',up))person=dense_code(decode_number(packed_number(up),user_key));else{auto li=id.generic_nodes.find(std::string(f[1]));if(li!=id.generic_nodes.end())person=id.parent[li->second];else{auto gi=g_generic_extra_codes.find(std::string(f[1]));if(gi!=g_generic_extra_codes.end())person=gi->second;}}
        if(person==UINT32_MAX||person+1>=session_offsets.size()||session_offsets[person]==session_offsets[person+1]){pos=e+1;continue;}
        uint64_t kid=0,packed=0;uint16_t generic=0;if(parse_packed(f[0],'k',packed))kid=packed;else{generic=uint16_t(tid+1);kid=w.generic_ids.size();w.generic_ids.emplace_back(f[0]);}
        uint32_t bucket=std::min<uint32_t>(CLICK_BUCKETS-1,person/person_stride);
        w.rows[bucket].push_back({person,uint32_t(tm-(ws-int64_t(7*DAY_MS))),uint16_t(camp),generic,kid});++w.stats.retained;pos=e+1;
      }
    }
  });
  for(auto&t:threads)t.join();
  for(auto&w:workers){out.physical+=w.stats.physical;out.structural_bad+=w.stats.structural_bad;out.empty+=w.stats.empty;out.invalid+=w.stats.invalid;out.retained+=w.stats.retained;}
  out.scan_seconds=std::chrono::duration<double>(Clock::now()-begun).count(); begun=Clock::now();
  auto less_id=[&](FastClick const&a,FastClick const&b){char ab[13],bb[13];std::string_view av=a.generic?std::string_view(workers[a.generic-1].generic_ids[a.id]):IdentityMap::packed_view(a.id,ab);std::string_view bv=b.generic?std::string_view(workers[b.generic-1].generic_ids[b.id]):IdentityMap::packed_view(b.id,bb);return av<bv;};
  struct Cell{uint64_t sessions=0,purchasing=0,revenue=0,assists=0;};unsigned ndays=unsigned((we-ws+DAY_MS-1)/DAY_MS);size_t nc=campaigns.ids.size()+1;std::vector<std::vector<Cell>> local(ncpu,std::vector<Cell>(nc*ndays));std::atomic<uint32_t> np{0};std::atomic<uint64_t> sort_ns{0};threads.clear();auto sweep_begun=Clock::now();
  for(int tid=0;tid<ncpu;++tid)threads.emplace_back([&,tid]{std::vector<uint32_t>active,active_pos(campaigns.ids.size(),UINT32_MAX),active_count(campaigns.ids.size());while(true){uint32_t bucket=np.fetch_add(1);if(bucket>=CLICK_BUCKETS)break;uint32_t p0=bucket*person_stride,p1=std::min<uint32_t>(g_person_codes,p0+person_stride);if(p0>=p1)continue;std::vector<uint32_t>counts(p1-p0);size_t total=0;for(auto const&w:workers)for(auto const&r:w.rows[bucket]){++counts[r.person-p0];++total;}std::vector<uint32_t>offsets(size_t(p1-p0)+1);uint32_t sum=0;for(uint32_t p=p0;p<p1;++p){offsets[p-p0]=sum;sum+=counts[p-p0];counts[p-p0]=offsets[p-p0];}offsets[p1-p0]=sum;std::vector<FastClick>grouped(total);for(auto const&w:workers)for(auto const&r:w.rows[bucket])grouped[counts[r.person-p0]++]=r;
    for(uint32_t p=p0;p<p1;++p){uint32_t s0=session_offsets[p],s1=session_offsets[p+1];if(s0==s1)continue;for(uint32_t c:active){active_count[c]=0;active_pos[c]=UINT32_MAX;}active.clear();auto b=grouped.begin()+offsets[p-p0],e=grouped.begin()+offsets[p-p0+1];auto sort_begun=Clock::now();std::sort(b,e,[](auto const&a,auto const&b){return a.ts<b.ts;});sort_ns.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-sort_begun).count(),std::memory_order_relaxed);size_t add=0,remove=0;int64_t latest=-1;
      for(uint32_t si=s0;si<s1;++si){uint32_t st=sessions[si].start+uint32_t(7*DAY_MS);while(add<size_t(e-b)&&b[add].ts<=st){auto const&r=b[add];if(active_count[r.campaign]++==0){active_pos[r.campaign]=active.size();active.push_back(r.campaign);}if(latest<0||r.ts>b[latest].ts||(r.ts==b[latest].ts&&less_id(r,b[latest])))latest=add;++add;}while(remove<add&&uint64_t(b[remove].ts)+7*DAY_MS<st){uint16_t c=b[remove].campaign;if(--active_count[c]==0){uint32_t at=active_pos[c],last=active.back();active[at]=last;active_pos[last]=at;active.pop_back();active_pos[c]=UINT32_MAX;}++remove;}unsigned day=sessions[si].start/DAY_MS;uint16_t credit=uint16_t(campaigns.ids.size());if(latest>=0&&uint64_t(b[latest].ts)+7*DAY_MS>=st)credit=b[latest].campaign;Cell&cell=local[tid][size_t(credit)*ndays+day];++cell.sessions;cell.purchasing+=sessions[si].revenue!=0;cell.revenue+=sessions[si].revenue;for(uint32_t c:active)++local[tid][size_t(c)*ndays+day].assists;}
    }} });
  for(auto&t:threads)t.join();out.sort_seconds=double(sort_ns.load())/1e9;out.sweep_seconds=std::chrono::duration<double>(Clock::now()-sweep_begun).count();auto reduce_begun=Clock::now();for(size_t i=0;i<nc*ndays;++i){Cell sumcell;for(auto const&v:local){sumcell.sessions+=v[i].sessions;sumcell.purchasing+=v[i].purchasing;sumcell.revenue+=v[i].revenue;sumcell.assists+=v[i].assists;}out.credited+=sumcell.sessions;out.assisted+=sumcell.assists;out.revenue+=sumcell.revenue;if(campaign_out&&(sumcell.sessions||sumcell.assists))campaign_out->push_back({uint16_t(i/ndays),uint16_t(i%ndays),uint32_t(sumcell.sessions),uint32_t(sumcell.purchasing),uint32_t(sumcell.assists),sumcell.revenue});}out.reduce_seconds=std::chrono::duration<double>(Clock::now()-reduce_begun).count();out.tail_seconds=std::chrono::duration<double>(Clock::now()-begun).count();return out;
}

static std::string base36_id(char prefix,uint64_t n){char buf[32];unsigned k=0;do{unsigned d=n%36;n/=36;buf[k++]=char(d<10?'0'+d:'a'+d-10);}while(n);std::string s(1,prefix);while(k)s.push_back(buf[--k]);return s;}
static std::string person_name(uint32_t code,IdentityMap const&id,uint64_t user_key,uint32_t person_bound){
  if(code<g_linked_base){uint32_t root=id.parent[code];return id.bytes(id.min_node[root]);}
  uint64_t ordinary=uint64_t(person_bound)*9,rel=uint64_t(code)-g_linked_base;if(rel<ordinary){uint64_t q=(rel/9)*512+(rel%9);return base36_id('u',scramble_number(q,user_key));}
  rel-=ordinary;if(rel<g_dense_extra_values.size())return base36_id('u',scramble_number(g_dense_extra_values[rel],user_key));rel-=g_dense_extra_values.size();return rel<g_generic_extra_values.size()?g_generic_extra_values[rel]:std::string();
}
static std::string date_string(int64_t epoch_day){epoch_day+=719468;int era=int((epoch_day>=0?epoch_day:epoch_day-146096)/146097);unsigned doe=unsigned(epoch_day-int64_t(era)*146097),yoe=(doe-doe/1460+doe/36524-doe/146096)/365;int y=int(yoe)+era*400;unsigned doy=doe-(365*yoe+yoe/4-yoe/100),mp=(5*doy+2)/153,d=doy-(153*mp+2)/5+1,m=mp+(mp<10?3:-9);y+=m<=2;char b[16];std::snprintf(b,sizeof(b),"%04d-%02u-%02u",y,m,d);return b;}
static void write_debug_csv(fs::path dir,std::vector<UserDailyTemp>const&users,std::vector<CampaignDailyTemp>const&campaigns,IdentityMap const&id,uint64_t user_key,uint32_t person_bound,int64_t ws,ProductCatalog const&products,Campaigns const&catalog){
  fs::create_directories(dir);int64_t wsday=ws/int64_t(DAY_MS);std::ofstream u(dir/"user_daily.csv",std::ios::binary);u<<"user_id,day,sessions,events,revenue_cents,top_category\n";for(auto const&r:users){u<<person_name(r.person,id,user_key,person_bound)<<','<<date_string(wsday+r.day)<<','<<r.sessions<<','<<r.events<<','<<r.revenue<<',';if(r.top_category!=UINT16_MAX)u<<products.categories[r.top_category];u<<'\n';}
  std::ofstream c(dir/"campaign_daily.csv",std::ios::binary);c<<"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n";for(auto const&r:campaigns){bool organic=r.campaign==catalog.ids.size();c<<(organic?"organic":catalog.ids[r.campaign])<<','<<(organic?"organic":catalog.channels[r.campaign])<<','<<date_string(wsday+r.day)<<','<<r.sessions<<','<<r.purchasing<<','<<r.revenue<<','<<r.assists<<'\n';}
}
struct OutputTimings{double names_s=0,rows_s=0,ffi_s=0;};
static int write_parquet_ffi(std::string const&out,std::vector<UserDailyTemp>const&users,std::vector<CampaignDailyTemp>const&campaign_rows,IdentityMap const&id,uint64_t user_key,uint32_t person_bound,int64_t ws,ProductCatalog const&products,Campaigns const&catalog,int ncpu,OutputTimings*timings=nullptr){
  auto stage=Clock::now();
  std::vector<std::string> names(users.size());std::vector<std::thread>ts;for(int t=0;t<ncpu;++t)ts.emplace_back([&,t]{for(size_t i=users.size()*t/ncpu,e=users.size()*(t+1)/ncpu;i<e;++i)names[i]=person_name(users[i].person,id,user_key,person_bound);});for(auto&t:ts)t.join();
  if(timings)timings->names_s=std::chrono::duration<double>(Clock::now()-stage).count();stage=Clock::now();
  int64_t epoch_day=ws/int64_t(DAY_MS);std::vector<FeUserRow>uw;uw.reserve(users.size());for(size_t i=0;i<users.size();++i){auto const&r=users[i];const std::string*top=r.top_category==UINT16_MAX?nullptr:&products.categories[r.top_category];uw.push_back({reinterpret_cast<uint8_t const*>(names[i].data()),uint32_t(names[i].size()),int32_t(epoch_day+r.day),r.sessions,r.events,r.revenue,top?reinterpret_cast<uint8_t const*>(top->data()):nullptr,top?uint32_t(top->size()):0});}
  static const std::string organic="organic";std::vector<FeCampaignRow>cw;cw.reserve(campaign_rows.size());for(auto const&r:campaign_rows){bool org=r.campaign==catalog.ids.size();auto const&cid=org?organic:catalog.ids[r.campaign];auto const&channel=org?organic:catalog.channels[r.campaign];cw.push_back({reinterpret_cast<uint8_t const*>(cid.data()),uint32_t(cid.size()),reinterpret_cast<uint8_t const*>(channel.data()),uint32_t(channel.size()),int32_t(epoch_day+r.day),r.sessions,r.purchasing,r.revenue,r.assists});}
  if(timings)timings->rows_s=std::chrono::duration<double>(Clock::now()-stage).count();stage=Clock::now();int rc=fe_write_outputs(out.c_str(),uw.data(),uw.size(),cw.data(),cw.size());if(timings)timings->ffi_s=std::chrono::duration<double>(Clock::now()-stage).count();return rc;
}

int main() {
  auto process_started=Clock::now();
  fs::path data = std::getenv("DATA_DIR") ? std::getenv("DATA_DIR") : "bench20m";
  int ncpu = std::getenv("NCPU") ? std::max(1, std::atoi(std::getenv("NCPU"))) : 8;
  ncpu = std::min(ncpu, 8);
  bool verify_pack = std::getenv("VERIFY_PACK") != nullptr;
  bool fingerprints = std::getenv("FASTENGINE_FINGERPRINT_OUT") != nullptr;
  int bench_mode = std::getenv("BENCH_MODE") ? std::atoi(std::getenv("BENCH_MODE")) : 0;
  int64_t ws, we;
  if (!manifest_window(data, ws, we)) {
    std::cerr << "bad manifest\n";
    return 2;
  }

  auto setup_started=Clock::now();std::vector<Task> tasks;
  uint64_t input_bytes = 0;
  for (auto const &entry : fs::directory_iterator(data / "events")) {
    if (!entry.is_regular_file() || entry.path().extension() != ".csv") continue;
    uint64_t size = entry.file_size();
    input_bytes += size;
    int fd = ::open(entry.path().c_str(), O_RDONLY);
    if (fd >= 0) { ::posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED); ::close(fd); }
    for (uint64_t b = 0; b < size; b += CHUNK)
      tasks.push_back(Task{entry.path(), size, b, std::min<uint64_t>(size, b + CHUNK)});
  }
  std::sort(tasks.begin(), tasks.end(), [](auto const &a, auto const &b) {
    return (a.end - a.begin) > (b.end - b.begin);
  });
  double setup_s=std::chrono::duration<double>(Clock::now()-setup_started).count();

  auto recover_started = Clock::now();
  const size_t estimated_rows = size_t(input_bytes / 76 + 1024);
  uint32_t dense_bound = uint32_t(std::min<size_t>(estimated_rows, std::numeric_limits<uint32_t>::max()));
  std::vector<uint64_t> samples = sample_numbers(data / "events", 0, 'e', 8);
  RecoveredKey event_recovered = recover_event_key(samples, dense_bound);
  uint64_t dense_key = event_recovered.key;
  uint32_t person_bound = uint32_t(std::max<size_t>(20, estimated_rows / 25 + 10'000));
  uint32_t user_dense_bound = person_bound * 512;
  std::vector<uint64_t> user_samples = sample_numbers(data / "events", 1, 'u', 24);
  RecoveredKey user_recovered = recover_user_key(user_samples, person_bound);
  uint64_t user_dense_key = user_recovered.key;
  double recover_s = std::chrono::duration<double>(Clock::now() - recover_started).count();
  auto links_started = Clock::now();
  auto links_future = std::async(std::launch::async, [&] {
    return IdentityMap::build(data / "links", user_dense_key, user_recovered.valid, person_bound,
      [](std::string_view s,char p,uint64_t&v){return parse_packed(s,p,v);},
      [](uint64_t n,uint64_t k){return decode_number(n,k);},
      [](uint64_t q,uint64_t k){return scramble_number(q,k);});
  });

  auto started = Clock::now();
  std::atomic<size_t> next{0};
  int scan_workers=std::max(1,ncpu-1);
  std::vector<Worker> workers{size_t(scan_workers)};
  const size_t rows_per_worker = estimated_rows / size_t(scan_workers) + 1024;
  for (auto &w : workers) {
    w.products.reserve(rows_per_worker / 24 + 64);
    w.generic.reserve(rows_per_worker / 400 + 64);
  }
  std::vector<std::thread> threads;
  for (int tid = 0; tid < scan_workers; ++tid) {
    threads.emplace_back([&, tid] {
      while (true) {
        size_t i = next.fetch_add(1, std::memory_order_relaxed);
        if (i >= tasks.size()) break;
        process_task(tasks[i], workers[size_t(tid)], unsigned(tid), ws, we, verify_pack,
                     bench_mode, dense_key, dense_bound, event_recovered.valid, user_dense_key,
                     user_dense_bound, user_recovered.valid, fingerprints);
      }
    });
  }
  for (auto &t : threads) t.join();
  if (bench_mode == 0) for (auto &w : workers) w.finish_events();
  double parse_s = std::chrono::duration<double>(Clock::now() - started).count();
  DedupStats dedup{};
  double dedup_s = 0;
  if (bench_mode == 0 && event_recovered.valid) {
    auto dedup_started = Clock::now();
    dedup = reduce_dense_slots(workers, dense_bound, ncpu, fingerprints, nullptr);
    dedup_s = std::chrono::duration<double>(Clock::now() - dedup_started).count();
  }
  auto link_wait_started=Clock::now();IdentityMap identities = links_future.get();double link_wait_s=std::chrono::duration<double>(Clock::now()-link_wait_started).count();
  double links_wall_s = std::chrono::duration<double>(Clock::now() - links_started).count();
  SessionKernelStats session_stats{}; double session_s=0;std::vector<CompactSession> compact_sessions;std::vector<uint32_t> session_offsets;std::vector<UserDailyTemp> user_daily;
  if(bench_mode==0&&event_recovered.valid&&user_recovered.valid&&!identities.domain_failed){auto ss=Clock::now();session_stats=build_sessions(data,workers,identities,person_bound,ncpu,ws,we,user_dense_key,fingerprints,&compact_sessions,&session_offsets,&user_daily,nullptr);session_s=std::chrono::duration<double>(Clock::now()-ss).count();}
  ClickResult click_stats{};std::vector<CampaignDailyTemp> campaign_daily;if(bench_mode==0&&!compact_sessions.empty())click_stats=run_clicks(data,ncpu,ws,we,user_dense_key,person_bound,identities,compact_sessions,session_offsets,&campaign_daily);
  if(bench_mode==0)if(const char*debug=std::getenv("DEBUG_CSV_DIR"))write_debug_csv(debug,user_daily,campaign_daily,identities,user_dense_key,person_bound,ws,load_products(data),read_campaigns(data));
  double output_s=0,output_catalog_s=0;OutputTimings output_detail{};if(bench_mode==0)if(const char*outdir=std::getenv("OUTPUT_DIR")){auto os=Clock::now();auto oc=Clock::now();auto output_products=load_products(data);auto output_campaigns=read_campaigns(data);output_catalog_s=std::chrono::duration<double>(Clock::now()-oc).count();if(write_parquet_ffi(outdir,user_daily,campaign_daily,identities,user_dense_key,person_bound,ws,output_products,output_campaigns,ncpu,&output_detail)!=0)return 3;output_s=std::chrono::duration<double>(Clock::now()-os).count();}
  double pipeline_s=std::chrono::duration<double>(Clock::now()-process_started).count();

  Stats total;
  uint64_t capacity_bytes = 0;
  for (auto const &w : workers) {
    total.physical += w.stats.physical;
    total.structural_bad += w.stats.structural_bad;
    total.empty_id += w.stats.empty_id;
    total.semantic_bad += w.stats.semantic_bad;
    total.compact += w.stats.compact;
    total.valid_compact += w.stats.valid_compact;
    total.generic += w.stats.generic;
    total.purchases += w.stats.purchases;
    total.boundary_grows += w.stats.boundary_grows;
    total.checksum ^= w.stats.checksum;
    capacity_bytes += w.products.capacity() * sizeof(ProductExtra);
    for (unsigned part = 0; part < PARTITIONS; ++part)
      capacity_bytes += w.events[part].capacity() * sizeof(CompactEvent);
    for (unsigned owner = 0; owner < DEDUP_OWNERS; ++owner)
      capacity_bytes += w.dedup[owner].capacity() * sizeof(DenseAux);
  }
  std::cerr << "event_input_bytes=" << input_bytes << " tasks=" << tasks.size() << " scan_workers=" << scan_workers << " workers=" << ncpu << '\n';
  std::cerr << "links_accepted=" << identities.accepted << " links_malformed=" << identities.malformed
            << " link_ids=" << identities.names.size() << " link_domain_failed=" << identities.domain_failed
            << " links_overlap_wall_seconds=" << links_wall_s << " link_wait_seconds="<<link_wait_s<<'\n';
  std::cerr << "bench_mode=" << bench_mode << '\n';
  std::cerr << "dense_key=" << dense_key << " dense_bound=" << dense_bound
            << " dense_ready=" << event_recovered.valid
            << " user_dense_key=" << user_dense_key << " user_dense_bound=" << user_dense_bound
            << " user_dense_ready=" << user_recovered.valid
            << " setup_seconds="<<setup_s<<" recover_seconds=" << recover_s << '\n';
  std::cerr << "physical=" << total.physical << " structural_bad=" << total.structural_bad
            << " empty_id=" << total.empty_id << " semantic_bad=" << total.semantic_bad << '\n';
  std::cerr << "compact_candidates=" << total.compact << " valid_compact=" << total.valid_compact
            << " generic=" << total.generic
            << " purchases=" << total.purchases << " boundary_grows=" << total.boundary_grows << '\n';
  std::cerr << "record_bytes=" << sizeof(CompactEvent) << " aux_bytes=" << sizeof(DenseAux)
            << " reserved_mib=" << (capacity_bytes >> 20) << " checksum=" << total.checksum << '\n';
  std::cerr << "parse_seconds=" << parse_s << " throughput_gbps=" << (double(input_bytes) / parse_s / 1e9) << '\n';
  if (bench_mode == 0)
    std::cerr << "dedup_unique=" << dedup.unique << " dedup_valid=" << dedup.valid
              << " dedup_checksum=" << dedup.checksum << " dedup_seconds=" << dedup_s << '\n';
  if(bench_mode==0)std::cerr<<"session_people="<<session_stats.people<<" sessions="<<session_stats.sessions<<" session_events="<<session_stats.events<<" session_revenue="<<session_stats.revenue<<" user_daily_rows="<<user_daily.size()<<" session_checksum="<<session_stats.checksum<<" session_seconds="<<session_s<<" session_catalog_seconds="<<session_stats.catalog_s<<" session_prep_seconds="<<session_stats.prep_s<<" count_seconds="<<session_stats.count_s<<" scatter_seconds="<<session_stats.scatter_s<<" kernel_seconds="<<session_stats.kernel_s<<" session_finalize_seconds="<<session_stats.finalize_s<<'\n';
  if(bench_mode==0)std::cerr<<"click_physical="<<click_stats.physical<<" click_bad="<<click_stats.structural_bad<<" click_empty="<<click_stats.empty<<" click_invalid="<<click_stats.invalid<<" click_retained="<<click_stats.retained<<" credited="<<click_stats.credited<<" assisted="<<click_stats.assisted<<" click_revenue="<<click_stats.revenue<<" campaign_daily_rows="<<campaign_daily.size()<<" click_catalog_seconds="<<click_stats.catalog_seconds<<" click_scan_seconds="<<click_stats.scan_seconds<<" click_tail_seconds="<<click_stats.tail_seconds<<" click_sort_worker_seconds="<<click_stats.sort_seconds<<" click_sweep_wall_seconds="<<click_stats.sweep_seconds<<" click_reduce_seconds="<<click_stats.reduce_seconds<<'\n';
  if(bench_mode==0&&std::getenv("OUTPUT_DIR"))std::cerr<<"output_seconds="<<output_s<<" output_catalog_seconds="<<output_catalog_s<<" name_materialize_seconds="<<output_detail.names_s<<" output_row_build_seconds="<<output_detail.rows_s<<" parquet_ffi_seconds="<<output_detail.ffi_s<<'\n';
  std::cerr<<"pipeline_to_output_seconds="<<pipeline_s<<'\n';
  if (const char *fingerprint = std::getenv("FASTENGINE_FINGERPRINT_OUT")) {
    std::ofstream fp(fingerprint, std::ios::binary | std::ios::trunc);
    fp << "{\"phase\":\"scan\",\"physical_rows\":" << total.physical
       << ",\"structural_bad\":" << total.structural_bad
       << ",\"empty_id\":" << total.empty_id << "}\n";
    fp << "{\"phase\":\"event_candidates\",\"compact\":" << total.compact
       << ",\"valid_compact\":" << total.valid_compact
       << ",\"generic\":" << total.generic
       << ",\"checksum\":" << total.checksum << "}\n";
    if (bench_mode == 0)
      fp << "{\"phase\":\"events_dedup\",\"unique\":" << dedup.unique
         << ",\"valid\":" << dedup.valid << ",\"checksum\":" << dedup.checksum << "}\n";
  }
  std::cerr<<"internal_total_seconds="<<std::chrono::duration<double>(Clock::now()-process_started).count()<<'\n';return 0;
}

