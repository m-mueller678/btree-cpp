#include "TlxWrapper.h"
#include <iostream>
#include <span>
#include <tlx/container/btree_map.hpp>
#include <vector>
#include "../btree/tuple.hpp"

uint32_t loadInt(uint8_t const* src)
{
   union {
      uint32_t i;
      uint8_t b[4];
   } x;
   memcpy(x.b, src, 4);
   return __builtin_bswap32(x.i);
}

void storeInt(uint8_t* dst, uint32_t x)
{
   uint32_t swapped = __builtin_bswap32(x);
   memcpy(dst, &swapped, 4);
}

struct TlxImpl {
   tlx::btree_map<std::vector<std::uint8_t>, std::vector<std::uint8_t>> strings;
   tlx::btree_map<std::uint32_t, std::vector<std::uint8_t>> ints;
   bool isInt;

   TlxImpl(bool isInt) : strings{}, ints{}, isInt(isInt) {}
};

TlxWrapper::TlxWrapper(bool isInt) : impl(new TlxImpl(isInt)) {}

uint8_t* TlxWrapper::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut)
{
   static uint8_t EmptyPayload = 0;
   if (impl->isInt) {
      auto it = impl->ints.find(loadInt(key));
      if (it == impl->ints.end()) {
         return nullptr;
      } else {
         payloadSizeOut = it->second.size();
         if (payloadSizeOut == 0)
            return &EmptyPayload;
         return it->second.data();
      }
   } else {
      std::vector keyVector(key, key + keyLength);
      auto it = impl->strings.find(keyVector);
      if (it == impl->strings.end()) {
         return nullptr;
      } else {
         payloadSizeOut = it->second.size();
         if (payloadSizeOut == 0)
            return &EmptyPayload;
         return it->second.data();
      }
   }
}

void TlxWrapper::insertImpl(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength)
{
   std::vector<uint8_t> value{payload, payload + payloadLength};
   if (impl->isInt) {
      impl->ints.insert(std::make_pair(loadInt(key), std::vector<uint8_t>(payload, payload + payloadLength)));
   } else {
      impl->strings.insert(std::make_pair(std::vector<uint8_t>(key, key + keyLength), std::vector<uint8_t>(payload, payload + payloadLength)));

      //      std::cout<<"Dump:"<<impl->strings.size()<<std::endl;
      //            for(auto const& x:impl->strings) {
      //              std::cout.write(reinterpret_cast<const char*>(x.first.data()), x.first.size());
      //              std::cout<<" _:_ ";
      //              std::cout.write(reinterpret_cast<const char*>(x.second.data()), x.second.size());
      //               std::cout << std::endl;
      //            }
   }
}

bool TlxWrapper::removeImpl(uint8_t* key, unsigned int keyLength) const
{
   abort();
}

void TlxWrapper::range_lookupImpl(uint8_t* key,
                                  unsigned int keyLen,
                                  uint8_t* keyOut,
                                  const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   if (impl->isInt) {
      auto it = impl->ints.lower_bound(loadInt(key));
      while (true) {
         if (it == impl->ints.end())
            break;
         storeInt(keyOut, it->first);
         if (!found_record_cb(4, it->second.data(), it->second.size())) {
            break;
         }
         ++it;
      }
   } else {
      std::vector keyVector(key, key + keyLen);
      auto it = impl->strings.lower_bound(keyVector);
      while (true) {
         if (it == impl->strings.end())
            break;
         if (it->first.size() > 0)
            memcpy(keyOut, it->first.data(), it->first.size());
         if (!found_record_cb(it->first.size(), it->second.data(), it->second.size())) {
            break;
         }
         ++it;
      }
   }
}

void TlxWrapper::range_lookup_descImpl(uint8_t* key,
                                       unsigned int keyLen,
                                       uint8_t* keyOut,
                                       const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   abort();
}

TlxWrapper::~TlxWrapper()
{
   delete impl;
}
