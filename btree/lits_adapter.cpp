#include "lits_adapter.hpp"
#include "../in-memory-structures/lits/lits/lits.hpp"
#include "tuple.hpp"

LitsBTreeAdapter::LitsBTreeAdapter(bool isInt)
{
   lits = new lits::LITS();
}

void LitsBTreeAdapter::insertImpl(uint8_t* key, unsigned int keyLength, uint8_t* payload, unsigned int payloadLength)
{
   if (payloadLength != 8)
      abort();
   uint64_t payload_copy;
   memcpy(&payload_copy, payload, 8);
   lits->upsert((char*)key, payload_copy);
}

uint8_t* LitsBTreeAdapter::lookupImpl(uint8_t* key, unsigned int keyLength, unsigned int& payloadSizeOut)
{
   lits::kv* found = lits->lookup((char*)key);
   if (!found)
      return nullptr;
   payloadSizeOut = 8;
   return reinterpret_cast<uint8_t*>(&found->v);
}

bool LitsBTreeAdapter::removeImpl(uint8_t* key, unsigned int keyLength) const
{
   abort();
}

void LitsBTreeAdapter::range_lookupImpl(uint8_t* key,
                                        unsigned int keyLen,
                                        uint8_t* keyOut,
                                        const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   auto it = lits->find((const char*)(key));
   while (it.not_finish()) {
      unsigned len = strlen(it.getKV()->k);
      memcpy(keyOut, it.getKV()->k, len);
      if (!found_record_cb(keyLen, reinterpret_cast<uint8_t*>(&it.getKV()->v), 8)) {
         break;
      }
      it.next();
   }
}

void LitsBTreeAdapter::range_lookup_descImpl(uint8_t* key,
                                             unsigned int keyLen,
                                             uint8_t* keyOut,
                                             const std::function<bool(unsigned int, uint8_t*, unsigned int)>& found_record_cb)
{
   abort();
}

void LitsBTreeAdapter::bulkInsert(std::vector<std::string>& keys)
{
   auto insertKeyCount = keys.size() / 2;
   if (insertKeyCount < 1000) {
      if (keys.size() < 1000)
         abort();
      else
         insertKeyCount = 1000;
   }
   std::vector<char*> insertKeys;
   std::vector<uint64_t> insertVals;
   for (int i = 0; i < insertKeyCount; ++i) {
      insertKeys.push_back(keys[i].data());
      insertVals.push_back(1);
   }
   std::sort(insertKeys.begin(), insertKeys.end(), [](char* a, char* b) { return lits::ustrcmp(a, b) < 0; });
   auto unique_it = std::unique(insertKeys.begin(), insertKeys.end(), [](char* a, char* b) { return std::strcmp(a, b) == 0; });
   // Erase the remaining duplicates after std::unique
   insertKeys.erase(unique_it, insertKeys.end());
   bool success = lits->bulkload((const char**)insertKeys.data(), insertVals.data(), insertKeys.size());
   if (!success)
      abort();
}