#pragma once

#include <cstdint>
#include <functional>
#include "tuple.hpp"
#include <string>
#include <vector>

namespace lits{
   struct LITS;
}

struct LitsBTreeAdapter {
   lits::LITS* lits;

   LitsBTreeAdapter(bool isInt);
   uint8_t* lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut);
   void insertImpl(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);
   bool removeImpl(uint8_t* key, unsigned int keyLength) const;
   void range_lookupImpl(uint8_t* key,
                         unsigned int keyLen,
                         uint8_t* keyOut,
                         const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   void range_lookup_descImpl(uint8_t* key,
                              unsigned int keyLen,
                              uint8_t* keyOut,
                              const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   void bulkInsert(std::vector<std::string>& keys);
};
