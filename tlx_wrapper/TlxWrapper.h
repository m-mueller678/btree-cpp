#ifndef BTREE_CPP_TLXWRAPPER_H
#define BTREE_CPP_TLXWRAPPER_H

#include <cstdint>
#include <functional>

struct TlxImpl;

struct TlxWrapper {
   TlxImpl* impl;

   TlxWrapper(bool isInt);
   ~TlxWrapper();
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
};


#endif  // BTREE_CPP_TLXWRAPPER_H
