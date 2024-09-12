#include "lits_adapter.hpp"
#include "../in-memory-structures/lits/lits/lits.hpp"
#include "tuple.hpp"

LitsBTreeAdapter::LitsBTreeAdapter(bool isInt){
   lits = new lits::LITS();
}

void LitsBTreeAdapter::insertImpl(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength)
{
   uintptr_t tuple = Tuple::makeLitsTuple(keyLength, payload, payloadLength);
   lits->upsert((char*)key,tuple);
}

uint8_t* LitsBTreeAdapter::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut)
{
   lits::kv* found = lits->lookup((char*)key);
   if(!found)
      return nullptr;
   auto tuple= found->v;
   payloadSizeOut = Tuple::tuplePayloadLen(tuple);
   // tuple does not contain key, payload is where key would usually be.
   return Tuple::tupleKeyPtr(tuple);
}

bool LitsBTreeAdapter::removeImpl(uint8_t* key, unsigned int keyLength) const
{
   abort();
}

void LitsBTreeAdapter::range_lookupImpl(uint8_t* key, unsigned int keyLen, uint8_t* keyOut, const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   auto it = lits->find((const char *)(key));
   while (it.not_finish()) {
      Tuple* tuple= reinterpret_cast<Tuple*>(it.getKV()->v);
      memcpy(keyOut, it.getKV()->k,tuple->keyLen);
      if (!found_record_cb(tuple->keyLen, tuple->data, tuple->payloadLen)) {
         break;
      }
      it.next();
   }
}

void LitsBTreeAdapter::range_lookup_descImpl(uint8_t* key, unsigned int keyLen, uint8_t* keyOut, const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   abort();
}

void LitsBTreeAdapter::bulkInsert(std::vector<std::string>& keys)
{
   auto insertKeyCount = keys.size()/2;
   if(insertKeyCount<1000){
      if (keys.size() < 1000)
         abort();
      else
         insertKeyCount = 1000;
   }
   std::vector<char*> insertKeys;
   std::vector<uint64_t> insertVals;
   for(int i=0;i<insertKeyCount;++i){
      insertKeys.push_back(keys[i].data());
      insertVals.push_back(1);
   }
   std::sort(insertKeys.begin(), insertKeys.end(), [](char* a, char* b) {
      return lits::ustrcmp(a, b) < 0;
   });
   auto unique_it = std::unique(insertKeys.begin(), insertKeys.end(), [](char* a, char* b) {
      return std::strcmp(a, b) == 0;
   });
   // Erase the remaining duplicates after std::unique
   insertKeys.erase(unique_it, insertKeys.end());
   bool success = lits->bulkload((const char**)insertKeys.data(),insertVals.data(),insertKeys.size());
   if(!success)
      abort();
}