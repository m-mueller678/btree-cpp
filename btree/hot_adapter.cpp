#include "hot_adapter.hpp"
#include "../in-memory-structures/hot/libs/hot/single-threaded/include/hot/singlethreaded/HOTSingleThreaded.hpp"
#include "../in-memory-structures/hot/libs/hot/single-threaded/include/hot/singlethreaded/HOTSingleThreadedInterface.hpp"
#include "tuple.hpp"

struct TupleKeyRef {
   uint8_t* data;
   unsigned length;

   bool operator==(const TupleKeyRef& rhs) const { return length == rhs.length && memcmp(data, rhs.data, length) == 0; }
   bool operator<(const TupleKeyRef& rhs) const { return std::basic_string_view{data, length} < std::basic_string_view{rhs.data, rhs.length}; }
};

template <typename T>
struct HotTupleKeyExtractor {
   template <typename K>
   TupleKeyRef operator()(K);
};

template <>
template <>
TupleKeyRef HotTupleKeyExtractor<Tuple*>::operator()(Tuple* t)
{
   return TupleKeyRef{t->data, t->keyLen};
};

template <>
template <>
TupleKeyRef HotTupleKeyExtractor<Tuple*>::operator()(const TupleKeyRef& k)
{
   return k;
};

template <>
template <>
TupleKeyRef HotTupleKeyExtractor<Tuple*>::operator()(TupleKeyRef k)
{
   return k;
};

template <typename T>
struct HotTupleIntKeyExtractor {
   template <typename K>
   std::array<uint8_t ,8> operator()(K);
};

template <>
template <>
std::array<uint8_t ,8> HotTupleIntKeyExtractor<std::array<uint8_t ,8>>::operator()(std::array<uint8_t ,8> t)
{
   return t;
};

typedef hot::singlethreaded::HOTSingleThreaded<Tuple*, HotTupleKeyExtractor> HotSS;
typedef hot::singlethreaded::HOTSingleThreaded<std::array<uint8_t ,8> , HotTupleIntKeyExtractor> HotSSI;

struct Hot {
   HotSS string_hot;
   HotSSI int_hot;
   bool isInt;
};

namespace idx
{
namespace contenthelpers
{
template <>
inline auto toFixSizedKey(TupleKeyRef const& key)
{
   constexpr size_t maxLen = getMaxKeyLength<char const*>();
   std::array<uint8_t, maxLen> fixedSizeKey;
   assert(key.length <= maxLen);
   memcpy(fixedSizeKey.data(), key.data, key.length);
   memset(fixedSizeKey.data() + key.length, 0, maxLen - key.length);
   return fixedSizeKey;
}

template <>
constexpr inline size_t getMaxKeyLength<TupleKeyRef>()
{
   return getMaxKeyLength<char const*>();
}
}  // namespace contenthelpers
}  // namespace idx

static uint8_t fake_payload=42;

uint8_t* HotBTreeAdapter::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut)
{
   if (hot->isInt) {
      auto it = hot->int_hot.find(*reinterpret_cast<std::array<uint8_t, 8>*>(key));
      if (it == HotSSI::END_ITERATOR)
         return nullptr;
      payloadSizeOut = 8;
      return &fake_payload;
   } else {
      auto it = hot->string_hot.find(TupleKeyRef{key, keyLength});
      if (it == HotSS::END_ITERATOR)
         return nullptr;
      Tuple* tuple = *it;
      payloadSizeOut = tuple->payloadLen;
      return tuple->payload();
   }
}

void HotBTreeAdapter::insertImpl(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength)
{
   if (hot->isInt) {
      uintptr_t tuple = Tuple::makeTuple(key, keyLength, payload, payloadLength);
      bool success = hot->int_hot.insert(*reinterpret_cast<std::array<uint8_t, 8>*>(key));
      assert(success);
   } else {
      uintptr_t tuple = Tuple::makeTuple(key, keyLength, payload, payloadLength);
      bool success = hot->string_hot.insert(reinterpret_cast<Tuple*>(tuple));
      assert(success);
   }
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
   if (hot->isInt) {
      abort();
   } else {
      auto it = hot->string_hot.lower_bound(TupleKeyRef{key, keyLen});
      while (true) {
         if (it == HotSS::END_ITERATOR)
            break;
         Tuple* tuple = *it;
         memcpy(keyOut, tuple->data, tuple->keyLen);
         if (!found_record_cb(tuple->keyLen, tuple->payload(), tuple->payloadLen)) {
            break;
         }
         ++it;
      }
   }
}

void HotBTreeAdapter::range_lookup_descImpl(uint8_t* key,
                                            unsigned int keyLen,
                                            uint8_t* keyOut,
                                            const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   abort();
}

HotBTreeAdapter::HotBTreeAdapter(bool isInt) : hot(new Hot{HotSS{}, HotSSI{}, isInt}) {}
