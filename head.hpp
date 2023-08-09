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
   }
}

//     debug_assert!(self.head.key_count < self.head.key_capacity);
//     if let Some(key) = Head::make_fence_head(key) {
//         let (head, keys, children, _) = self.as_parts_mut();
//         keys[..head.key_count as usize + 1]
//         .copy_within(index..head.key_count as usize, index + 1);
//         children[..head.key_count as usize + 2]
//         .copy_within(index..head.key_count as usize + 1, index + 1);
//         keys[index] = key;
//         children[index] = child;
//         head.key_count += 1;
//         self.update_hint(index);
//         Ok(())
//     } else {
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
//     }

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
