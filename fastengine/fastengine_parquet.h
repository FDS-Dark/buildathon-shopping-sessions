#ifndef FASTENGINE_PARQUET_H
#define FASTENGINE_PARQUET_H

#include <cstddef>
#include <cstdint>

extern "C" {

struct FeUserRow {
  const std::uint8_t* user;
  std::uint32_t user_len;
  std::int32_t day;
  std::uint32_t sessions;
  std::uint32_t events;
  std::uint64_t revenue;
  const std::uint8_t* top;
  std::uint32_t top_len;
};

struct FeCampaignRow {
  const std::uint8_t* campaign;
  std::uint32_t campaign_len;
  const std::uint8_t* channel;
  std::uint32_t channel_len;
  std::int32_t day;
  std::uint32_t sessions;
  std::uint32_t purchasing;
  std::uint64_t revenue;
  std::uint32_t assists;
};

int fe_write_outputs(const char* output_dir, const FeUserRow* users,
                     std::size_t user_count,
                     const FeCampaignRow* campaigns,
                     std::size_t campaign_count);

}  // extern "C"

static_assert(sizeof(FeUserRow) == 48);
static_assert(sizeof(FeCampaignRow) == 56);

#endif


