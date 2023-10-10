#include "hot_adapter.hpp"
#include "../in-memory-structures/hot/libs/hot/single-threaded/include/hot/singlethreaded/HOTSingleThreaded.hpp"
#include "../in-memory-structures/hot/libs/hot/single-threaded/include/hot/singlethreaded/HOTSingleThreadedInterface.hpp"
#include "tuple.hpp"

template <typename T>
struct HotTupleKeyExtractor {
   template <typename K>
   uint8_t* operator()(K);
};

template <>
template <>
uint8_t* HotTupleKeyExtractor<Tuple*>::operator()<Tuple*>(Tuple* t)
{
   return t->data;
};

template <>
template <>
uint8_t* HotTupleKeyExtractor<Tuple*>::operator()<uint8_t* const&>(uint8_t* const& t)
{
   return t;
};

template <>
template <>
uint8_t* HotTupleKeyExtractor<Tuple*>::operator()<uint8_t*>(uint8_t* t)
{
   return t;
};

struct Hot {
   hot::singlethreaded::HOTSingleThreaded<Tuple*, HotTupleKeyExtractor> hot;
};

uint8_t* HotBTreeAdapter::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut)
{
   auto it = hot->hot.find(key);
   Tuple* tuple = *it;
   payloadSizeOut = tuple->keyLen;
   return tuple->payload();
}

void HotBTreeAdapter::insertImpl(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength)
{
   uintptr_t tuple = Tuple::makeTuple(key, keyLength, payload, payloadLength);
   hot->hot.insert(reinterpret_cast<Tuple*>(tuple));
}
bool HotBTreeAdapter::removeImpl(uint8_t* key, unsigned int keyLength) const
{
   abort();
}
void HotBTreeAdapter::range_lookupImpl(uint8_t* key,
                                       unsigned int keyLen,
                                       uint8_t* keyOut,
                                       const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   abort();
}
void HotBTreeAdapter::range_lookup_descImpl(uint8_t* key,
                                            unsigned int keyLen,
                                            uint8_t* keyOut,
                                            const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   abort();
}

HotBTreeAdapter::HotBTreeAdapter() : hot(new Hot()) {}
