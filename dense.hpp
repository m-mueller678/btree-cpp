#include <cstdint>

struct DenseNodeHeader {
    uint8_t tag;
    uint16_t full_key_len;
    uint32_t array_start;
    uint16_t val_len;
    uint16_t slot_count;
    uint16_t lower_fence_len;
    uint16_t upper_fence_len;
    uint16_t prefix_len;
}

static unsigned min(unsigned a, unsigned b)
{
   return a < b ? a : b;
}

static unsigned min(unsigned a, unsigned b)
{
    return a < b ? b : a;
}


struct DenseNode : public BTreeNodeHeader {
   union {
        uint64_t mask[(pageSize - sizeof(DenseNodeHeader)) / sizeof(uint64_t)];
        uint8_t heap[pageSize - sizeof(DenseNodeHeader)];
   };
   uint8_t* getLowerFence() { return ptr() + pageSize - lower_fence_len }
   uint8_t* getUpperFence() { return ptr() + pageSize - lower_fence_len - max(upper_fence_len,full_key_len)}
   uint8_t* getPrefix() { return getLowerFence() }
}
