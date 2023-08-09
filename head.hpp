#include "btree2020.hpp"

template <class T>
void HeadNode<T>::destroy()
{
   abort();
}

template <class T>
bool HeadNode<T>::makeSepHead(uint8_t* key, unsigned keyLen, T* out)
{
   uint8_t* bytes = reinterpret_cast<uint8_t*>(out);
   memset(bytes, 0, sizeof(T));
   if (keyLen < sizeof(T)) {
      memcpy(bytes, key, keyLen);
      bytes[sizeof(T) - 1] = keyLen;
      *out = byteswap(*out);
      return true;
   } else {
      return false;
   }
}

template <class T>
void HeadNode<T>::makeNeedleHead(uint8_t* key, unsigned keyLen, T* out)
{
   uint8_t* bytes = reinterpret_cast<uint8_t*>(out);
   memset(bytes, 0, sizeof(T));
   if (keyLen < sizeof(T)) {
      memcpy(bytes, key, keyLen);
      bytes[sizeof(T) - 1] = keyLen;
   } else {
      memcpy(bytes, key, sizeof(T) - 1);
      bytes[sizeof(T) - 1] = sizeof(T);
   }
   *out = byteswap(*out);
}

template <class T>
void HeadNode<T>::splitNode(AnyNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
{
   abort();
}

template <class T>
bool HeadNode<T>::insertChild(uint8_t* key, unsigned int keyLength, AnyNode* child)
{
   T head;
   if (makeSepHead(key, keyLength, &head)) {
      bool found;
      unsigned index = lowerBound(head, found);
      ASSUME(!found);
      insertAt(index, head, child);
   } else {
      abort();  // TODO
      //         let data = NodeDataBuffer::new();
      //         let fences = data.store_fences(self.fences());
      //         self.export(&data, PrefixMode::Keep);
      //         let this = unsafe { reinterpret_mut::<Self, BTreeNode>(self) };
      //         <BasicNode as NodeCreator<true>>::create(
      //             this,
      //             fences,
      //             &data,
      //             0..data.len(),
      //                                                  PrefixMode::Keep,
      //         )?;
      //         let this = unsafe { reinterpret_mut::<BTreeNode, BasicNode>(this) };
      //         Self::try_insert_to_basic(this, index, key, child)
   }
}

template <class T>
void HeadNode<T>::insertAt(unsigned index, T head, AnyNode* child)
{
   memmove(keys + index + 1, keys + index, (count - index) * sizeof(T));
   keys[index] = head;
   memmove(children() + index + 1, children() + index, (count - index) * sizeof(AnyNode*));
   storeUnaligned(children() + index, child);
}

AnyNode** HeadNodeHead::children()
{
   return reinterpret_cast<AnyNode**>(ptr() + childOffset);
}

uint8_t* HeadNodeHead::ptr()
{
   return reinterpret_cast<uint8_t*>(this);
}

template <class T>
unsigned HeadNode<T>::lowerBound(T head, bool& foundOut)
{
   foundOut = false;
   // check hint
   unsigned lower = 0;
   unsigned upper = count;
   // searchHint(keyHead, lower, upper);
   while (lower < upper) {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (head <= keys[mid]) {
         upper = mid;
      } else if (head > keys[mid]) {
         lower = mid + 1;
      } else {
         foundOut = true;
         return mid;
      }
   }
   return lower;
}

template <class T>
bool HeadNode<T>::requestSpaceFor(unsigned keyLen)
{
   abort();
}
template <class T>
void HeadNode<T>::getSep(uint8_t* sepKeyOut, BTreeNode::SeparatorInfo info)
{
   abort();
}
template <class T>
AnyNode* HeadNode<T>::lookupInner(uint8_t* key, unsigned keyLength)
{
   abort();
}
