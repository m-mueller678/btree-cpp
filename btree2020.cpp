#include <cassert>
#include <cstdint>
#include <cstring>
#include "btree2020.hpp"

static unsigned min(unsigned a, unsigned b)
{
   return a < b ? a : b;
}

template <class T>
static T loadUnaligned(void* p)
{
   T x;
   memcpy(&x, p, sizeof(T));
   return x;
}

// Get order-preserving head of key (assuming little endian)
static uint32_t head(uint8_t* key, unsigned keyLength)
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


   BTreeNode::BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) {}

   uint8_t* BTreeNode::ptr() { return reinterpret_cast<uint8_t*>(this); }
   bool BTreeNode::isInner() { return !isLeaf; }
   uint8_t* BTreeNode::getLowerFence() { return ptr() + lowerFence.offset; }
   uint8_t* BTreeNode::getUpperFence() { return ptr() + upperFence.offset; }
   uint8_t* BTreeNode::getPrefix() { return ptr() + lowerFence.offset; } // any key on page is ok

   unsigned BTreeNode::freeSpace() { return dataOffset - (reinterpret_cast<uint8_t*>(slot + count) - ptr()); }
   unsigned BTreeNode::freeSpaceAfterCompaction() { return pageSize - (reinterpret_cast<uint8_t*>(slot + count) - ptr()) - spaceUsed; }

   bool BTreeNode::requestSpaceFor(unsigned spaceNeeded)
   {
      if (spaceNeeded <= freeSpace())
         return true;
      if (spaceNeeded <= freeSpaceAfterCompaction()) {
         compactify();
         return true;
      }
      return false;
   }

   BTreeNode* BTreeNode::makeLeaf() { return new BTreeNode(true); }
   BTreeNode* BTreeNode::makeInner() { return new BTreeNode(false); }

   uint8_t* BTreeNode::getKey(unsigned slotId) { return ptr() + slot[slotId].offset; }
   uint8_t* BTreeNode::getPayload(unsigned slotId) { return ptr() + slot[slotId].offset + slot[slotId].keyLen; }

   BTreeNode* BTreeNode::getChild(unsigned slotId)
   {
      assert(isInner());
      return loadUnaligned<BTreeNode*>(getPayload(slotId));
   }

   // How much space would inserting a new key of length "keyLength" require?
   unsigned BTreeNode::spaceNeeded(unsigned keyLength, unsigned payloadLength)
   {
      assert(keyLength >= prefixLength); // fence key logic makes it impossible to insert a key that is shorter than prefix
      return sizeof(Slot) + (keyLength - prefixLength) + payloadLength;
   }

   void BTreeNode::makeHint()
   {
      unsigned dist = count / (hintCount + 1);
      for (unsigned i = 0; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].head;
   }

   void BTreeNode::updateHint(unsigned slotId)
   {
      unsigned dist = count / (hintCount + 1);
      unsigned begin = 0;
      if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
         begin = (slotId / dist) - 1;
      for (unsigned i = begin; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].head;
   }

   void BTreeNode::searchHint(uint32_t keyHead, unsigned& lowerOut, unsigned& upperOut)
   {
      if (count > hintCount * 2) {
         unsigned dist = upperOut / (hintCount + 1);
         unsigned pos, pos2;
         for (pos = 0; pos < hintCount; pos++)
            if (hint[pos] >= keyHead)
               break;
         for (pos2 = pos; pos2 < hintCount; pos2++)
            if (hint[pos2] != keyHead)
               break;
         lowerOut = pos * dist;
         if (pos2 < hintCount)
            upperOut = (pos2 + 1) * dist;
      }
   }

   // lower bound search, foundOut indicates if there is an exact match, returns slotId
   unsigned BTreeNode::lowerBound(uint8_t* key, unsigned keyLength, bool& foundOut)
   {
      foundOut = false;

      // skip prefix
      assert(memcmp(key, getPrefix(), min(keyLength, prefixLength))==0);
      key += prefixLength;
      keyLength -= prefixLength;

      // check hint
      unsigned lower = 0;
      unsigned upper = count;
      uint32_t keyHead = head(key, keyLength);
      searchHint(keyHead, lower, upper);

      // binary search on remaining range
      while (lower < upper) {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
         } else { // head is equal, check full key
            int cmp = memcmp(key, getKey(mid), min(keyLength, slot[mid].keyLen));
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               if (keyLength < slot[mid].keyLen) { // key is shorter
                  upper = mid;
               } else if (keyLength > slot[mid].keyLen) { // key is longer
                  lower = mid + 1;
               } else {
                  foundOut = true;
                  return mid;
               }
            }
         }
      }
      return lower;
   }

   // lowerBound wrapper ignoring exact match argument (for convenience)
   unsigned BTreeNode::lowerBound(uint8_t* key, unsigned keyLength)
   {
      bool ignore;
      return lowerBound(key, keyLength, ignore);
   }

   bool BTreeNode::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
   {
      if (!requestSpaceFor(spaceNeeded(keyLength, payloadLength)))
         return false;  // no space, insert fails
      unsigned slotId = lowerBound(key, keyLength);
      memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
      storeKeyValue(slotId, key, keyLength, payload, payloadLength);
      count++;
      updateHint(slotId);
      return true;
   }

   bool BTreeNode::removeSlot(unsigned slotId)
   {
      spaceUsed -= slot[slotId].keyLen;
      spaceUsed -= slot[slotId].payloadLen;
      memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
      count--;
      makeHint();
      return true;
   }

   bool BTreeNode::remove(uint8_t* key, unsigned keyLength)
   {
      bool found;
      unsigned slotId = lowerBound(key, keyLength, found);
      if (!found)
         return false;
      return removeSlot(slotId);
   }

   void BTreeNode::compactify()
   {
      unsigned should = freeSpaceAfterCompaction();
      static_cast<void>(should);
      BTreeNode tmp(isLeaf);
      tmp.setFences(getLowerFence(), lowerFence.length, getUpperFence(), upperFence.length);
      copyKeyValueRange(&tmp, 0, 0, count);
      tmp.upper = upper;
      memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(BTreeNode));
      makeHint();
      assert(freeSpace() == should);
   }

   // merge "this" into "right" via "tmp"
   bool BTreeNode::mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right)
   {
      if (isLeaf) {
         assert(right->isLeaf);
         assert(parent->isInner());
         BTreeNode tmp(isLeaf);
         tmp.setFences(getLowerFence(), lowerFence.length, right->getUpperFence(), right->upperFence.length);
         unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
         unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
         unsigned spaceUpperBound =
             spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
         if (spaceUpperBound > pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<uint8_t*>(right), &tmp, sizeof(BTreeNode));
         right->makeHint();
         return true;
      } else {
         assert(right->isInner());
         assert(parent->isInner());
         BTreeNode tmp(isLeaf);
         tmp.setFences(getLowerFence(), lowerFence.length, right->getUpperFence(), right->upperFence.length);
         unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
         unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
         unsigned extraKeyLength = parent->prefixLength + parent->slot[slotId].keyLen;
         unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow + tmp.spaceNeeded(extraKeyLength, sizeof(BTreeNode*));
         if (spaceUpperBound > pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         uint8_t extraKey[extraKeyLength];
         memcpy(extraKey, parent->getLowerFence(), parent->prefixLength);
         memcpy(extraKey+parent->prefixLength, parent->getKey(slotId), parent->slot[slotId].keyLen);
         storeKeyValue(count, extraKey, extraKeyLength, parent->getPayload(slotId), parent->slot[slotId].payloadLen);
         count++;
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<uint8_t*>(right), &tmp, sizeof(BTreeNode));
         return true;
      }
   }

   // store key/value pair at slotId
   void BTreeNode::storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
   {
      // slot
      key += prefixLength;
      keyLength -= prefixLength;
      slot[slotId].head = head(key, keyLength);
      slot[slotId].keyLen = keyLength;
      slot[slotId].payloadLen = payloadLength;
      // key
      unsigned space = keyLength + payloadLength;
      dataOffset -= space;
      spaceUsed += space;
      slot[slotId].offset = dataOffset;
      assert(getKey(slotId) >= reinterpret_cast<uint8_t*>(&slot[slotId]));
      memcpy(getKey(slotId), key, keyLength);
      memcpy(getPayload(slotId), payload, payloadLength);
   }

   void BTreeNode::copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount)
   {
      if (prefixLength <= dst->prefixLength) {  // prefix grows
         unsigned diff = dst->prefixLength - prefixLength;
         for (unsigned i = 0; i < srcCount; i++) {
            unsigned newKeyLength = slot[srcSlot + i].keyLen - diff;
            unsigned space = newKeyLength + slot[srcSlot + i].payloadLen;
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot + i].offset = dst->dataOffset;
            uint8_t* key = getKey(srcSlot + i) + diff;
            memcpy(dst->getKey(dstSlot + i), key, space);
            dst->slot[dstSlot + i].head = head(key, newKeyLength);
            dst->slot[dstSlot + i].keyLen = newKeyLength;
            dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
         }
      } else {
         for (unsigned i = 0; i < srcCount; i++)
            copyKeyValue(srcSlot + i, dst, dstSlot + i);
      }
      dst->count += srcCount;
      assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
   }

   void BTreeNode::copyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot)
   {
      unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
      uint8_t key[fullLength];
      memcpy(key, getPrefix(), prefixLength);
      memcpy(key+prefixLength, getKey(srcSlot), slot[srcSlot].keyLen);
      dst->storeKeyValue(dstSlot, key, fullLength, getPayload(srcSlot), slot[srcSlot].payloadLen);
   }

   void BTreeNode::insertFence(FenceKeySlot& fk, uint8_t* key, unsigned keyLength)
   {
      assert(freeSpace() >= keyLength);
      dataOffset -= keyLength;
      spaceUsed += keyLength;
      fk.offset = dataOffset;
      fk.length = keyLength;
      memcpy(ptr() + dataOffset, key, keyLength);
   }

   void BTreeNode::setFences(uint8_t* lowerKey, unsigned lowerLen, uint8_t* upperKey, unsigned upperLen)
   {
      insertFence(lowerFence, lowerKey, lowerLen);
      insertFence(upperFence, upperKey, upperLen);
      for (prefixLength = 0; (prefixLength < min(lowerLen, upperLen)) && (lowerKey[prefixLength] == upperKey[prefixLength]); prefixLength++)
         ;
   }

   void BTreeNode::splitNode(BTreeNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
   {
      // split this node into nodeLeft and nodeRight
      assert(sepSlot > 0);
      assert(sepSlot < (pageSize / sizeof(BTreeNode*)));
      BTreeNode* nodeLeft = new BTreeNode(isLeaf);
      nodeLeft->setFences(getLowerFence(), lowerFence.length, sepKey, sepLength);
      BTreeNode tmp(isLeaf);
      BTreeNode* nodeRight = &tmp;
      nodeRight->setFences(sepKey, sepLength, getUpperFence(), upperFence.length);
      bool succ = parent->insert(sepKey, sepLength, reinterpret_cast<uint8_t*>(&nodeLeft), sizeof(BTreeNode*));
      static_cast<void>(succ);
      assert(succ);
      if (isLeaf) {
         copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
         copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
      } else {
         // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
         copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
         copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
         nodeLeft->upper = getChild(nodeLeft->count);
         nodeRight->upper = upper;
      }
      nodeLeft->makeHint();
      nodeRight->makeHint();
      memcpy(reinterpret_cast<char*>(this), nodeRight, sizeof(BTreeNode));
   }

   unsigned BTreeNode::commonPrefix(unsigned slotA, unsigned slotB)
   {
      assert(slotA < count);
      unsigned limit = min(slot[slotA].keyLen, slot[slotB].keyLen);
      uint8_t *a = getKey(slotA), *b = getKey(slotB);
      unsigned i;
      for (i = 0; i < limit; i++)
         if (a[i] != b[i])
            break;
      return i;
   }

   BTreeNode::SeparatorInfo BTreeNode::findSeparator()
   {
      assert(count > 1);
      if (isInner()) {
         // inner nodes are split in the middle
         unsigned slotId = count / 2;
         return BTreeNode::SeparatorInfo{static_cast<unsigned>(prefixLength + slot[slotId].keyLen), slotId, false};
      }

      // find good separator slot
      unsigned bestPrefixLength, bestSlot;
      if (count > 16) {
         unsigned lower = (count / 2) - (count / 16);
         unsigned upper = (count / 2);

         bestPrefixLength = commonPrefix(lower, 0);
         bestSlot = lower;

         if (bestPrefixLength != commonPrefix(upper - 1, 0))
            for (bestSlot = lower + 1; (bestSlot < upper) && (commonPrefix(bestSlot, 0) == bestPrefixLength); bestSlot++)
               ;
      } else {
         bestSlot = (count-1) / 2;
         bestPrefixLength = commonPrefix(bestSlot, 0);
      }

      // try to truncate separator
      unsigned common = commonPrefix(bestSlot, bestSlot + 1);
      if ((bestSlot + 1 < count) && (slot[bestSlot].keyLen > common) && (slot[bestSlot + 1].keyLen > (common + 1)))
         return SeparatorInfo{prefixLength + common + 1, bestSlot, true};

      return SeparatorInfo{static_cast<unsigned>(prefixLength + slot[bestSlot].keyLen), bestSlot, false};
   }

   void BTreeNode::getSep(uint8_t* sepKeyOut, SeparatorInfo info)
   {
      memcpy(sepKeyOut, getPrefix(), prefixLength);
      memcpy(sepKeyOut + prefixLength, getKey(info.slot + info.isTruncated), info.length - prefixLength);
   }

   BTreeNode* BTreeNode::lookupInner(uint8_t* key, unsigned keyLength)
   {
      unsigned pos = lowerBound(key, keyLength);
      if (pos == count)
         return upper;
      return getChild(pos);
   }

   void BTreeNode::destroy()
   {
      if (isInner()) {
         for (unsigned i = 0; i < count; i++)
            getChild(i)->destroy();
         upper->destroy();
      }
      delete this;
      return;
   }

BTree::BTree() : root(BTreeNode::makeLeaf()) {}

BTree::~BTree() { root->destroy(); }

// point lookup
uint8_t* BTree::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
   BTreeNode* node = root;
   while (node->isInner())
      node = node->lookupInner(key, keyLength);
   bool found;
   unsigned pos = node->lowerBound(key, keyLength, found);
   if (!found)
      return nullptr;

   // key found, copy payload
   assert(pos < node->count);
   payloadSizeOut = node->slot[pos].payloadLen;
   return node->getPayload(pos);
}

bool BTree::lookup(uint8_t* key, unsigned keyLength)
{
   unsigned x;
   return lookup(key, keyLength, x) != nullptr;
}

void BTree::splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength, unsigned payloadLength)
{
   // create new root if necessary
   if (!parent) {
      parent = BTreeNode::makeInner();
      parent->upper = node;
      root = parent;
   }

   // split
   BTreeNode::SeparatorInfo sepInfo = node->findSeparator();
   unsigned spaceNeededParent = parent->spaceNeeded(sepInfo.length, sizeof(BTreeNode*));
   if (parent->requestSpaceFor(spaceNeededParent)) {  // is there enough space in the parent for the separator?
      uint8_t sepKey[sepInfo.length];
      node->getSep(sepKey, sepInfo);
      node->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
   } else {
      // must split parent first to make space for separator, restart from root to do this
      ensureSpace(parent, key, keyLength, sizeof(BTreeNode*));
   }
}

void BTree::ensureSpace(BTreeNode* toSplit, uint8_t* key, unsigned keyLength, unsigned payloadLength)
{
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      while (node->isInner() && (node != toSplit)) {
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      splitNode(toSplit, parent, key, keyLength, payloadLength);
   }

void BTree::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   assert((keyLength+payloadLength) <= BTreeNode::maxKVSize);
   BTreeNode* node = root;
   BTreeNode* parent = nullptr;
   while (node->isInner()) {
      parent = node;
      node = node->lookupInner(key, keyLength);
   }
   if (node->insert(key, keyLength, payload, payloadLength))
      return;

   // node is full: split and restart
   splitNode(node, parent, key, keyLength, payloadLength);
   insert(key, keyLength, payload, payloadLength);
}

bool BTree::remove(uint8_t* key, unsigned keyLength)
{
   BTreeNode* node = root;
   BTreeNode* parent = nullptr;
   unsigned pos = 0;
   while (node->isInner()) {
      parent = node;
      pos = node->lowerBound(key, keyLength);
      node = (pos == node->count) ? node->upper : node->getChild(pos);
   }
   if (!node->remove(key, keyLength))
      return false;  // key not found

   // merge if underfull
   if (node->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
      // find neighbor and merge
      if (parent && (parent->count >= 2) && ((pos + 1) < parent->count)) {
         BTreeNode* right = parent->getChild(pos + 1);
         if (right->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
            if (node->mergeNodes(pos, parent, right))
               delete node;
         }
      }
   }
   return true;
}
