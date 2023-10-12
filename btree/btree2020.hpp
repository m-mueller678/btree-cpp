#pragma once
#pragma clang diagnostic ignored "-Wvla-extension"
#pragma clang diagnostic ignored "-Wzero-length-array"
#pragma clang diagnostic ignored "-Wzero-length-array"
#pragma clang diagnostic ignored "-Wgnu-anonymous-struct"
#pragma clang diagnostic ignored "-Wnested-anon-types"
#pragma clang diagnostic ignored "-Wc99-extensions"

#include <bit>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include "hot_adapter.hpp"
#include <map>
#include "config.hpp"

#ifndef NDEBUG
#define CHECK_TREE_OPS
#endif

inline unsigned min(unsigned a, unsigned b)
{
   return a < b ? a : b;
}

inline unsigned max(unsigned a, unsigned b)
{
   return a < b ? b : a;
}

#define COUNTER(name, val, freq)                                                                                         \
   {                                                                                                                     \
      static uint64_t name##_count = 0;                                                                                  \
      static uint64_t name##_sum = 0;                                                                                    \
      name##_count += 1;                                                                                                 \
      name##_sum += (val);                                                                                               \
      if (name##_count % (freq) == 0) {                                                                                  \
         printf("counter %20s| avg %.3e | sum %.3e | cnt %.3e\n", #name, static_cast<double>(name##_sum) / name##_count, \
                static_cast<double>(name##_sum), static_cast<double>(name##_count));                                     \
      }                                                                                                                  \
   }

#define ASSUME(x)          \
   {                       \
      assert(x);           \
      __builtin_assume(x); \
   }

template <class T>
static T loadUnaligned(void* p)
{
   T x;
   memcpy(&x, p, sizeof(T));
   return x;
}

template <class T>
static void storeUnaligned(void* p, T t)
{
   memcpy(p, &t, sizeof(T));
}

// Get order-preserving head of key (assuming little endian)
inline uint32_t head(uint8_t* key, unsigned keyLength)
{
   switch (keyLength) {
      case 0:
         return 0;
      case 1:
         return static_cast<uint32_t>(key[0]) << 24;
      case 2:
         return static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 16;
      case 3:
         return (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 16) | (static_cast<uint32_t>(key[2]) << 8);
      default:
         return __builtin_bswap32(loadUnaligned<uint32_t>(key));
   }
}

inline std::vector<uint8_t> toByteVector(uint8_t* b, unsigned l)
{
   return std::vector(b, b + l);
}

template <class T>
inline T byteswap(T x);

template <>
inline uint32_t byteswap<uint32_t>(uint32_t x)
{
   return __builtin_bswap32(x);
}

template <>
inline uint64_t byteswap<uint64_t>(uint64_t x)
{
   return __builtin_bswap64(x);
}

template <class T>
constexpr unsigned headNodeHintCount(T);

template <>
inline constexpr unsigned headNodeHintCount<uint32_t>(uint32_t)
{
   return headNode4HintCount;
}

template <>
inline constexpr unsigned headNodeHintCount<uint64_t>(uint64_t)
{
   return headNode8HintCount;
}

struct BTreeNode;
union AnyNode;

enum class Tag : uint8_t {
   Leaf = 0,
   Inner = 1,
   Dense = 2,
   Hash = 3,
   Head4 = 4,
   Head8 = 5,
   Dense2 = 6,
   _last = 6,
};

constexpr unsigned TAG_END = unsigned(Tag::_last) +1;
const char* tag_name(Tag tag);

struct BTreeNodeHeader {
   static constexpr unsigned hintCount = basicHintCount;
   static constexpr unsigned underFullSizeLeaf = pageSizeLeaf / 4;  // merge nodes below this size
   static constexpr unsigned underFullSizeInner = pageSizeInner / 4;  // merge nodes below this size

   Tag _tag;

   BTreeNodeHeader(){}
   BTreeNodeHeader(bool isLeaf);

   struct FenceKeySlot {
      uint16_t offset;
      uint16_t length;
   };

   AnyNode* upper = nullptr;  // only used in inner nodes

   FenceKeySlot lowerFence = {0, 0};  // exclusive
   FenceKeySlot upperFence = {0, 0};  // inclusive

   uint16_t count = 0;
   uint16_t spaceUsed = 0;
   uint16_t dataOffset;
   uint16_t prefixLength = 0;

   uint32_t hint[hintCount];
   uint32_t padding;
};

struct HashNode;

struct BTreeNode : public BTreeNodeHeader {
   struct Slot {
      uint16_t offset;
      uint16_t keyLen;
      uint16_t payloadLen;
      union {
         uint32_t head[enableBasicHead];
         uint8_t headBytes[enableBasicHead ? 4 : 0];
      };
   } __attribute__((packed));
   union {
      Slot slot[1]; // grows from front
      uint8_t heap[1]; // grows from back
   };

   // this struct does not have appropriate size.
   // Get Some storage location and call init.
   BTreeNode()=delete;
   static constexpr unsigned maxKVSize = ((pageSizeLeaf - sizeof(BTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

   void init(bool isLeaf);
   uint8_t* ptr();
   bool isInner();
   bool isLeaf();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();
   uint8_t* getPrefix();

   unsigned freeSpace();
   unsigned freeSpaceAfterCompaction();

   bool requestSpaceFor(unsigned spaceNeeded);

   static AnyNode* makeLeaf();

   uint8_t* getKey(unsigned slotId);
   uint8_t* getPayload(unsigned slotId);

   AnyNode* getChild(unsigned slotId);

   // How much space would inserting a new key of length "getKeyLength" require?
   unsigned spaceNeeded(unsigned keyLength, unsigned payloadLength);

   void makeHint();

   void updateHint(unsigned slotId);

   void searchHint(uint32_t keyHead, unsigned& lowerOut, unsigned& upperOut);

   // lower bound search, foundOut indicates if there is an exact match, returns slotId
   unsigned lowerBound(uint8_t* key, unsigned keyLength, bool& foundOut);

   // lowerBound wrapper ignoring exact match argument (for convenience)
   unsigned lowerBound(uint8_t* key, unsigned keyLength);

   bool insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   void removeSlot(unsigned slotId);

   bool remove(uint8_t* key, unsigned keyLength);

   void compactify();

   // merge "this" into "right" via "tmp"
   bool mergeNodes(unsigned slotId, AnyNode* parent, BTreeNode* right);

   // store key/value pair at slotId
   void storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   void copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount);

   void copyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot);

   void insertFence(FenceKeySlot& fk, uint8_t* key, unsigned keyLength);

   void setFences(uint8_t* lowerKey, unsigned lowerLen, uint8_t* upperKey, unsigned upperLen);

   void splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength);

   struct SeparatorInfo {
      unsigned length;   // length of new separator
      unsigned slot;     // slot at which we split
      bool isTruncated;  // if true, we truncate the separator taking length bytes from slot+1
   };

   unsigned commonPrefix(unsigned slotA, unsigned slotB);

   SeparatorInfo findSeparator();

   void getSep(uint8_t* sepKeyOut, SeparatorInfo info);

   AnyNode* lookupInner(uint8_t* key, unsigned keyLength);

   void destroy();

   AnyNode* any() { return reinterpret_cast<AnyNode*>(this); }
   bool is_underfull();
   bool insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child);
   bool range_lookup(uint8_t* key,
                     unsigned int keyLen,
                     uint8_t* keyOut,
                     const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   void validate_child_fences();
   void print();
   void restoreKey(uint8_t* keyOut, unsigned len, unsigned index);
   void validateHint();
   bool range_lookup_desc(uint8_t* key,
                          unsigned int keyLen,
                          uint8_t* keyOut,
                          const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   bool hasBadHeads();
   void splitToHash(AnyNode* parent, unsigned int sepSlot, uint8_t* sepKey, unsigned int sepLength);
   void copyKeyValueRangeToHash(HashNode* dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);
};

union TmpBTreeNode{
   BTreeNode node;
   uint8_t _bytes[std::max(pageSizeLeaf,pageSizeInner)];
   TmpBTreeNode(){}
};

typedef uint32_t NumericPart;
constexpr unsigned maxNumericPartLen = sizeof(NumericPart);

typedef uint64_t Mask;
constexpr unsigned maskBytesPerWord = sizeof(Mask);
constexpr unsigned maskBitsPerWord = 8 * maskBytesPerWord;

enum KeyError : int {
   WrongLen = -1,
   NotNumericRange = -2,
   SlightlyTooLarge = -3,
   FarTooLarge = -4,
};

struct DenseNode  {
   Tag tag;
   uint16_t fullKeyLen;
   NumericPart arrayStart;
   union{
      uint16_t spaceUsed;
      uint16_t valLen;
   };
   uint16_t slotCount;
   uint16_t occupiedCount;
   uint16_t lowerFenceLen;
   union {
      struct{
         uint16_t upperFenceLen;
         uint16_t prefixLength;
         uint16_t _mask_pad[2];
         Mask mask[(pageSizeLeaf - 24) / sizeof(Mask)];
      };
      struct{
         uint16_t _union_pad[2];
         uint16_t dataOffset;
         uint16_t slots[(pageSizeLeaf - 22)/sizeof (uint16_t)];
      };
      uint8_t _expand_heap[pageSizeLeaf - 16];
   };
   unsigned fencesOffset();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();

   AnyNode* any();

   uint8_t* getPrefix();

   void restoreKey(uint8_t* prefix, uint8_t* dst, unsigned index);

   void changeUpperFence(uint8_t* fence, unsigned len);

   void copyKeyValueRangeToBasic(BTreeNode* dst, unsigned srcStart, unsigned srcEnd);

   bool insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   void splitNode1(AnyNode* parent, uint8_t* key, unsigned keyLen);
   void splitNode2(AnyNode* parent, uint8_t* key, unsigned keyLen);

   unsigned prefixDiffLen();
   KeyError keyToIndex(uint8_t* truncatedKey, unsigned truncatedLen);

   static unsigned computeNumericPartLen(unsigned fullKeyLen);
   static unsigned computeNumericPrefixLength(unsigned fullKeyLen);

   void changeLowerFence(uint8_t* lowerFence, unsigned lowerFenceLen, uint8_t* upperFence, unsigned upperFenceLen);
   static bool densify1(DenseNode* out,BTreeNode* basicNode);
   static bool densify2(DenseNode* out,BTreeNode* from);
   void init2b(uint8_t* lowerFence, unsigned lowerFenceLen, uint8_t* upperFence, unsigned upperFenceLen, unsigned fullKeyLen,unsigned slotCount);
   int cmpNumericPrefix(uint8_t* key,unsigned length);

   unsigned maskWordCount();

   void zeroMask();
   void zeroSlots();

   // rounds down
   static NumericPart getNumericPart(uint8_t* key,unsigned length,unsigned targetLength);
   static NumericPart leastGreaterKey(uint8_t* key,unsigned length,unsigned targetLength);
   void updateArrayStart();

   uint8_t* ptr();

   static unsigned computeSlotCount(unsigned valLen, unsigned fencesStart);

   bool try_densify(BTreeNode* basicNode);

   bool isSlotPresent(unsigned i);

   void setSlotPresent(unsigned i);
   void insertSlotWithSpace(unsigned i,uint8_t* payload,unsigned payloadLen);
   bool requestSpaceFor(unsigned payloadLen);
   unsigned slotEndOffset();
   unsigned slotValLen(unsigned index);

   void unsetSlotPresent(unsigned i);

   uint8_t* getVal(unsigned i);
   uint8_t* lookup(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut);
   void updatePrefixLength();
   bool remove(uint8_t* key, unsigned int keyLength);
   bool is_underfull();
   BTreeNode* convertToBasic();
   bool range_lookup1(uint8_t* key,
                     unsigned int keyLen,
                     uint8_t* keyOut,
                     const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   bool range_lookup2(uint8_t* key,
                      unsigned int keyLen,
                      uint8_t* keyOut,
                      const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   bool range_lookup_desc(uint8_t* key,
                          unsigned int keyLen,
                          uint8_t* keyOut,
                          const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   bool isNumericRangeAnyLen(uint8_t* key,unsigned length);
   void print();
};

struct HashNodeHeader {
   Tag _tag;
   uint16_t count;
   uint16_t sortedCount;
   // includes keys, payloads, and fences
   // excludes slots and hash array
   uint16_t spaceUsed;
   uint16_t dataOffset;
   uint16_t prefixLength;
   uint16_t hashCapacity;
   uint16_t hashOffset;
   uint16_t lowerFenceLen;
   uint16_t upperFenceLen;
};

struct HashSlot {
   uint16_t offset;
   uint16_t keyLen;
   uint16_t payloadLen;
};

struct HashNode : public HashNodeHeader {
   union {
      HashSlot slot[(pageSizeLeaf - sizeof(HashNodeHeader)) / sizeof(HashSlot)];  // grows from front
      uint8_t heap[pageSizeLeaf - sizeof(HashNodeHeader)];                        // grows from back
   };

   unsigned estimateCapacity();
   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut);
   static uint8_t compute_hash(uint8_t* key, unsigned int keyLength);
   uint8_t* ptr();
   uint8_t* hashes();
   uint8_t* getPayload(unsigned int slotId);
   uint8_t* getKey(unsigned int slotId);
   void init(uint8_t* lowerFence, unsigned int lowerFenceLen, uint8_t* upperFence, unsigned int upperFenceLen, unsigned hashCapacity);
   static AnyNode* makeRootLeaf();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();
   void updatePrefixLength();
   bool insert(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength);
   int findIndex(uint8_t* key, unsigned keyLength, uint8_t hash);
   unsigned int freeSpace();
   unsigned int freeSpaceAfterCompaction();
   bool requestSpace(unsigned int spaceNeeded);
   bool requestSlotAndSpace(unsigned int spaceNeeded);
   void compactify(unsigned newHashCapacity);
   void sort();
   unsigned int commonPrefix(unsigned int slotA, unsigned int slotB);
   BTreeNode::SeparatorInfo findSeparator();
   void getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info);
   void splitNode(AnyNode* parent, unsigned int sepSlot, uint8_t* sepKey, unsigned int sepLength);
   AnyNode* any() { return reinterpret_cast<AnyNode*>(this); }
   void updateHash(unsigned int i);
   void copyKeyValue(unsigned srcSlot, HashNode* dst, unsigned dstSlot);
   void storeKeyValue(unsigned int slotId, uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength, uint8_t hash);
   void copyKeyValueRange(HashNode* dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);
   bool removeSlot(unsigned int slotId);
   bool remove(uint8_t* key, unsigned int keyLength);
   bool mergeNodes(unsigned int slotId, AnyNode* parent, HashNode* right);
   void print();
   bool range_lookup(uint8_t* key,
                     unsigned int keyLen,
                     uint8_t* keyOut,
                     const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   unsigned int lowerBound(uint8_t* key, unsigned int keyLength, bool& found);

   int findIndexNoSimd(uint8_t* key, unsigned keyLength, uint8_t hash);
   int findIndexSimd(uint8_t* key, unsigned keyLength, uint8_t hash);
   bool range_lookup_desc(uint8_t* key,
                          unsigned int keyLen,
                          uint8_t* keyOut,
                          const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   void validate();
   void copyKeyValueRangeToBasic(BTreeNode* dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);
   void copyKeyValueToBasic(unsigned int srcSlot, BTreeNode* dst, unsigned int dstSlot);
   void splitToBasic(AnyNode* parent, unsigned int sepSlot, uint8_t* sepKey, unsigned int sepLength);
   bool hasGoodHeads();
} __attribute__((aligned(hashUseSimd ? hashSimdWidth : 2)));

struct HeadNodeHead {
   Tag _tag;
   uint16_t count;
   uint16_t keyCapacity;
   uint16_t lowerFenceLen;
   uint16_t upperFenceLen;
   uint16_t prefixLength;
   uint8_t* ptr();
   uint8_t* getLowerFence();
   uint8_t* getUpperFence();
   void updatePrefixLength();
   unsigned int fencesOffset();
   AnyNode* any() { return reinterpret_cast<AnyNode*>(this); }
   static bool requestChildConvertFromBasic(BTreeNode* node, unsigned int newKeyLength);
};

template <class T>
struct HeadNode : public HeadNodeHead {
   static constexpr unsigned hintCount = headNodeHintCount<T>(0);
   uint32_t hint[hintCount];
   static constexpr unsigned paddedHeadSize = (sizeof(HeadNodeHead) + alignof(T) - 1) / alignof(T) * alignof(T) + sizeof(hint);
   union {
      T keys[(pageSizeInner - paddedHeadSize) / sizeof(T)];
      uint8_t data[pageSizeInner - paddedHeadSize];
   };

   void destroy();
   void splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength);
   bool insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child);
   bool requestSpaceFor(unsigned keyLen);
   void getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info);
   AnyNode* lookupInner(uint8_t* key, unsigned keyLength);
   static bool makeSepHead(uint8_t* key, unsigned int keyLen, T* out);
   static void makeNeedleHead(uint8_t* key, unsigned int keyLen, T* out);
   unsigned int lowerBound(T head, bool& foundOut);
   void insertAt(unsigned int index, T head, AnyNode* child);
   void init(uint8_t* lowerFence, unsigned int lowerFenceLen, uint8_t* upperFence, unsigned int upperFenceLen);
   AnyNode** children();
   void clone_from_basic(BTreeNode* src);
   unsigned getKeyLength(unsigned int i);
   void copyKeyValueRangeToBasic(BTreeNode* dst, unsigned int srcStart, unsigned int srcEnd);
   void copyKeyValueRange(HeadNode<T>* dst, unsigned int dstSlot, unsigned int srcSlot, unsigned int srcCount);
   bool convertToBasicWithSpace(unsigned int newKeyLen);
   BTreeNode::SeparatorInfo findSeparator();
   void print();
   unsigned lookupInnerIndex(uint8_t* key, unsigned keyLength);
   void restoreKey(uint8_t* keyOut, unsigned keyLen, unsigned index);
   void removeSlot(unsigned int slotId);
   void makeHint();
   void updateHint(unsigned int slotId);
   void searchHint(T keyHead, unsigned int& lowerOut, unsigned int& upperOut);
   void validateHint();
   bool convertToHead8WithSpace();
   bool convertToHead4WithSpace();
};

typedef HeadNode<uint32_t> HeadNode4;
typedef HeadNode<uint64_t> HeadNode8;

union AnyNode {
   Tag _tag;
   BTreeNode _basic_node;
   DenseNode _dense;
   HashNode _hash;
   HeadNode4 _head4;
   HeadNode8 _head8;

   Tag tag();

   AnyNode(BTreeNode basic);
   AnyNode();

   void destroy();

   void dealloc();
   static AnyNode* allocLeaf();
   static AnyNode* allocInner();

   bool isAnyInner();

   BTreeNode* basic();
   DenseNode* dense();
   HashNode* hash();
   HeadNode<uint32_t>* head4();
   HeadNode<uint64_t>* head8();
   bool insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child);
   bool innerRequestSpaceFor(unsigned keyLen);
   AnyNode* lookupInner(uint8_t* key, unsigned keyLength);
   static AnyNode* makeRoot(AnyNode* child);
   void print();
   unsigned lookupInnerIndex(uint8_t* key, unsigned keyLength);
   unsigned innerCount();
   AnyNode* getChild(unsigned index);
   void innerRestoreKey(uint8_t* keyOut, unsigned len, unsigned index);
   void innerRemoveSlot(unsigned int slotId);
   unsigned innerKeyLen(unsigned index);
   bool splitNodeWithParent(AnyNode* parent, uint8_t* key, unsigned keyLength);
   void nodeCount(unsigned counts[TAG_END]);
};

struct BTree {
   BTree();
   ~BTree();

   AnyNode* root;
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
   void splitNode(AnyNode* node, AnyNode* parent, uint8_t* key, unsigned keyLength);
   void ensureSpace(AnyNode* toSplit, uint8_t* key, unsigned keyLength);
};

namespace art
{
struct Node;
}

struct ArtBTreeAdapter {
   art::Node* root;
   ArtBTreeAdapter();
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

struct DataStructureWrapper {
   DataStructureWrapper();
#ifdef CHECK_TREE_OPS
   std::map<std::vector<uint8_t>, std::vector<uint8_t>> std_map;
#endif
#if defined(USE_STRUCTURE_BTREE)
   BTree impl;
#elif defined(USE_STRUCTURE_ART)
   ArtBTreeAdapter impl;
#elif defined(USE_STRUCTURE_HOT)
   HotBTreeAdapter impl;
#endif

   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut);
   bool lookup(uint8_t* key, unsigned keyLength)
   {
      unsigned ignore;
      return lookup(key, keyLength, ignore) != nullptr;
   }
   void insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);
   bool remove(uint8_t* key, unsigned keyLength);
   void range_lookup(uint8_t* key,
                     unsigned int keyLen,
                     uint8_t* keyOut,
                     const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   void range_lookup_desc(uint8_t* key,
                          unsigned int keyLen,
                          uint8_t* keyOut,
                          const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb);
   void testing_update_payload(uint8_t* key, unsigned int keyLength, uint8_t* payload);
};

void printKey(uint8_t* key, unsigned length);