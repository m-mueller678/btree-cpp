#include "wh_adapter.hpp"
#include "../in-memory-structures/wormhole/lib.h"
#include "../in-memory-structures/wormhole/kv.h"
#include "../in-memory-structures/wormhole/wh.h"
#include "tuple.hpp"


struct kv *
kvmap_mm_out_noop(struct kv * const kv, struct kv * const out)
{
   return kv;
}

const struct kvmap_mm kvmap_mm_btree = {
    .in = kvmap_mm_in_noop,
    .out = kvmap_mm_out_noop,
    .free = kvmap_mm_free_free,
    .priv = NULL,
};

WhBTreeAdapter::WhBTreeAdapter(bool isInt){
   wh = whunsafe_create(&kvmap_mm_btree);
}

uint8_t* WhBTreeAdapter::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut){
   struct kref kref;
   kref_ref_hash32(&kref, key, keyLength);
   struct kv * const kv =whunsafe_get(wh,&kref,nullptr);
   if (!kv)
      return nullptr;
   payloadSizeOut = kv->vlen;
   return kv->kv+kv->klen;
}

void WhBTreeAdapter::insertImpl(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength){
   struct kv * const newkv = kv_create(key, keyLength, payload, payloadLength);
   whunsafe_put(wh,newkv);
}

struct wormhole_iter {
   struct wormref * ref; // safe-iter only
   struct wormhole * map;
   struct wormleaf * leaf;
   u32 is;
};

bool WhBTreeAdapter::removeImpl(uint8_t* key, unsigned int keyLength) const{abort();}

void WhBTreeAdapter::range_lookupImpl(uint8_t* key,
                      unsigned int keyLen,
                      uint8_t* keyOut,
                      const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb){
   struct wormhole_iter iter;
   iter.ref = nullptr;
   iter.map = wh;
   iter.leaf = nullptr;
   iter.is = 0;
   struct kref kref;
   kref_ref_hash32(&kref, key, keyLen);
   whunsafe_iter_seek(&iter, &kref);
   while(true){
      struct kv * const kv = wormhole_iter_peek(&iter,nullptr);
      if(!kv)
         return;
      memcpy(keyOut,kv->kv,kv->klen);
      if (!found_record_cb(kv->klen,kv->kv+kv->klen,kv->vlen)){
         return;
      }
      whunsafe_iter_skip1(&iter);
   }

}
void WhBTreeAdapter::range_lookup_descImpl(uint8_t* key,
                           unsigned int keyLen,
                           uint8_t* keyOut,
                           const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb){abort();}