#include <cstdint>

// maximum page size (in bytes) is 65536
static const unsigned pageSize = 4096;

union AnyNode {
   uint8_t tag;
};
