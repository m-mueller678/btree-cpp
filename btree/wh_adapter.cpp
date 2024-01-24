#include "wh_adapter.hpp"
#include "../in-memory-structures/wormhole/lib.h"
#include "../in-memory-structures/wormhole/kv.h"
#include "../in-memory-structures/wormhole/wh.h"

#include "tuple.hpp"

WhBTreeAdapter::WhBTreeAdapter(bool isInt){abort();}
uint8_t* WhBTreeAdapter::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut){abort();}
void WhBTreeAdapter::insertImpl(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength){abort();}
bool WhBTreeAdapter::removeImpl(uint8_t* key, unsigned int keyLength) const{abort();}
void WhBTreeAdapter::range_lookupImpl(uint8_t* key,
                      unsigned int keyLen,
                      uint8_t* keyOut,
                      const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb){abort();}
void WhBTreeAdapter::range_lookup_descImpl(uint8_t* key,
                           unsigned int keyLen,
                           uint8_t* keyOut,
                           const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb){abort();}