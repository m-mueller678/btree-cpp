#include <algorithm>
#include <csignal>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include "btree/BtreeCppPerfEvent.hpp"
#include "btree/btree2020.hpp"
#include <iostream>

using namespace std;

extern "C" {
struct ZipfGenerator;
void zipf_generate(uint32_t, double, uint32_t*, uint32_t, bool);
}

// zipfParameter is assumed to not change between invocations.
unsigned zipf_next(BTreeCppPerfEvent& e, unsigned num_keys, double zipfParameter, bool shuffle, bool overGenerate)
{
   constexpr unsigned GEN_SIZE = 1 << 18;
   static unsigned ARRAY[GEN_SIZE];
   static unsigned index = GEN_SIZE - 1;
   static unsigned generatedNumKeys = 0;

   while (true) {
      // COUNTER(zipf_fail_rate, num_keys > generatedNumKeys, 1 << 10);
      if (num_keys > generatedNumKeys) {
         index = GEN_SIZE - 1;
      }
      index += 1;
      if (index == GEN_SIZE)
         index = 0;
      if (index == 0) {
         e.disableCounters();
         generatedNumKeys = num_keys + (overGenerate ? GEN_SIZE / 10 : 0);
         zipf_generate(generatedNumKeys, zipfParameter, ARRAY, GEN_SIZE, shuffle);
         e.enableCounters();
      }
      // COUNTER(zipf_reject_rate, ARRAY[index] >= num_keys, 1 << 10);
      if (ARRAY[index] < num_keys) {
         return ARRAY[index];
      }
   }
}

bool op_next(BTreeCppPerfEvent& e)
{
   constexpr unsigned GEN_SIZE = 5 * (1 << 18);
   static bool ARRAY[GEN_SIZE];
   static unsigned index = GEN_SIZE - 1;
   index += 1;
   if (index == GEN_SIZE)
      index = 0;
   if (index == 0) {
      e.disableCounters();
      unsigned trueCount = GEN_SIZE / 20;
      for (unsigned i = 0; i < GEN_SIZE; ++i)
         ARRAY[i] = i < trueCount;
      random_shuffle(ARRAY, ARRAY + GEN_SIZE);
      e.enableCounters();
   }
   return ARRAY[index];
}

uint64_t envu64(const char* env)
{
   if (getenv(env))
      return strtod(getenv(env), nullptr);
   std::cerr << "missing env " << env << std::endl;
   abort();
}

double envf64(const char* env)
{
   if (getenv(env))
      return strtod(getenv(env), nullptr);
   std::cerr << "missing env " << env << std::endl;
   abort();
}

uint8_t* makePayload(unsigned len)
{
   // add one to support zero length payload
   uint8_t* payload = new uint8_t[len + 1];
   memset(payload, 42, len);
   return payload;
}

bool isDataInt(BTreeCppPerfEvent& e)
{
   auto name = e.params["data_name"];
   return name == "int" || name == "rng4";
}

bool keySizeAcceptable(unsigned maxPayload,vector<string>& data){
   for(auto& k:data){
      if(k.size()+maxPayload>BTreeNode::maxKVSize)
         return false;
   }
   return true;
}

void runYcsbC(BTreeCppPerfEvent e, vector<string>& data, unsigned keyCount, unsigned payloadSize, unsigned opCount, double zipfParameter, bool dryRun)
{
   if (keyCount <= data.size() && keySizeAcceptable(payloadSize,data)) {
      if (!dryRun)
         random_shuffle(data.begin(), data.end());
      data.resize(keyCount);
   } else {
      std::cerr << "UNACCEPTABLE" << std::endl;
      keyCount = 0;
      opCount = 0;
   }

   uint8_t* payload = makePayload(payloadSize);

   DataStructureWrapper t(isDataInt(e));
   {
      // insert
      if (!dryRun)
         for (uint64_t i = 0; i < keyCount; i++) {
            uint8_t* key = (uint8_t*)data[i].data();
            unsigned int length = data[i].size();
            t.insert(key, length, payload, payloadSize);
            std::cout<<BTreeNode::leaf_count<<"\n";
         }
   }

   exit(0);

   {
      e.setParam("op", "ycsb_c");
      BTreeCppPerfEventBlock b(e, t, opCount);
      if (!dryRun)
         for (uint64_t i = 0; i < opCount; i++) {
            unsigned keyIndex = zipf_next(e, keyCount, zipfParameter, false, false);
            assert(keyIndex < data.size());
            unsigned payloadSizeOut;
            uint8_t* key = (uint8_t*)data[keyIndex].data();
            unsigned long length = data[keyIndex].size();
            uint8_t* payload = t.lookup(key, length, payloadSizeOut);
            if (!payload || (payloadSize != payloadSizeOut) || (payloadSize > 0 && *payload != 42))
               throw;
         }
   }

   data.clear();
}

void runSortedInsert(BTreeCppPerfEvent e, vector<string>& data, unsigned keyCount, unsigned payloadSize, bool dryRun, bool doSort = true)
{
   if (keyCount <= data.size() && keySizeAcceptable(payloadSize,data)) {
      data.resize(keyCount);
      if (!dryRun && doSort) {
         std::sort(data.begin(), data.end());
      }
   } else {
      std::cerr << "UNACCEPTABLE" << std::endl;
      keyCount = 0;
   }

   uint8_t* payload = makePayload(payloadSize);

   DataStructureWrapper t(isDataInt(e));
   {
      // insert
      e.setParam("op", "sorted_insert");
      BTreeCppPerfEventBlock b(e, t, keyCount);
      if (!dryRun)
         for (uint64_t i = 0; i < keyCount; i++) {
            uint8_t* key = (uint8_t*)data[i].data();
            unsigned int length = data[i].size();
            t.insert(key, length, payload, payloadSize);
         }
   }
   data.clear();
}

bool computeInitialKeyCount(unsigned avgKeyCount,
                            unsigned availableKeyCount,
                            unsigned opCount,
                            unsigned& initialKeyCount,
                            unsigned& reasonableMaxKeys)
{
   bool configValid = false;
   initialKeyCount = 0;
   if (avgKeyCount > opCount / 40) {
      initialKeyCount = avgKeyCount - opCount / 40;
      unsigned expectedInsertions = opCount / 20;
      reasonableMaxKeys = initialKeyCount + expectedInsertions * 2;
      if (reasonableMaxKeys <= availableKeyCount) {
         configValid = true;
      } else {
         std::cerr << "not enough keys" << std::endl;
      }
   } else {
      std::cerr << "too many ops for data size" << std::endl;
   }
   return configValid;
}

void runYcsbD(BTreeCppPerfEvent e,
              vector<string>& data,
              unsigned avgKeyCount,
              unsigned payloadSize,
              unsigned opCount,
              double zipfParameter,
              bool dryRun)
{
   unsigned initialKeyCount = 0;
   unsigned reasonableMaxKeys = 0;
   if (!computeInitialKeyCount(avgKeyCount, data.size(), opCount, initialKeyCount, reasonableMaxKeys) || ! keySizeAcceptable(payloadSize,data)) {
      opCount = 0;
      initialKeyCount = 0;
      data.resize(0);
   }

   if (!dryRun)
      random_shuffle(data.begin(), data.end());
   uint8_t* payload = makePayload(payloadSize);

   DataStructureWrapper t(isDataInt(e));
   {
      // insert
      e.setParam("op", "ycsb_d_init");
      BTreeCppPerfEventBlock b(e, t, initialKeyCount);
      if (!dryRun)
         for (uint64_t i = 0; i < initialKeyCount; i++) {
            uint8_t* key = (uint8_t*)data[i].data();
            unsigned int length = data[i].size();
            t.insert(key, length, payload, payloadSize);
         }
   }

   unsigned insertedCount = initialKeyCount;
   {
      e.setParam("op", "ycsb_d");
      BTreeCppPerfEventBlock b(e, t, opCount);
      if (!dryRun)
         for (uint64_t completedOps = 0; completedOps < opCount; ++completedOps) {
            if (op_next(e)) {
               if (insertedCount == data.size()) {
                  std::cerr << "exhausted keys for insertion" << std::endl;
                  abort();
               }
               uint8_t* key = (uint8_t*)data[insertedCount].data();
               unsigned int length = data[insertedCount].size();
               t.insert(key, length, payload, payloadSize);
               ++insertedCount;
            } else {
               unsigned zipfSample = zipf_next(e, insertedCount, zipfParameter, false, true);
               unsigned keyIndex = insertedCount - 1 - zipfSample;
               unsigned payloadSizeOut;
               uint8_t* key = (uint8_t*)data[keyIndex].data();
               unsigned long length = data[keyIndex].size();
               uint8_t* payload = t.lookup(key, length, payloadSizeOut);
               if (!payload || (payloadSize != payloadSizeOut) || (payloadSize > 0 && *payload != 42))
                  throw;
            }
         }
   }
}

void runYcsbE(BTreeCppPerfEvent e,
              vector<string>& data,
              unsigned avgKeyCount,
              unsigned payloadSize,
              unsigned opCount,
              unsigned maxScanLength,
              double zipfParameter,
              bool dryRun)
{
   unsigned initialKeyCount = 0;
   unsigned reasonableMaxKeys = 0;
   if (!computeInitialKeyCount(avgKeyCount, data.size(), opCount, initialKeyCount, reasonableMaxKeys) || ! keySizeAcceptable(payloadSize,data)) {
      opCount = 0;
      initialKeyCount = 0;
      data.resize(0);
   }

   if (!dryRun)
      random_shuffle(data.begin(), data.end());
   uint8_t* payload = makePayload(payloadSize);
   if (data.size() > 0) {  // permute zipf indices
      unsigned* permutation = new unsigned[data.size()];
      for (unsigned i = 0; i < data.size(); ++i) {
         permutation[i] = i;
      }
      random_shuffle(permutation, permutation + data.size());
      delete[] permutation;
   }
   DataStructureWrapper t(isDataInt(e));
   {
      // insert
      e.setParam("op", "ycsb_e_init");
      BTreeCppPerfEventBlock b(e, t, initialKeyCount);
      if (!dryRun)
         for (uint64_t i = 0; i < initialKeyCount; i++) {
            uint8_t* key = (uint8_t*)data[i].data();
            unsigned int length = data[i].size();
            t.insert(key, length, payload, payloadSize);
         }
   }

   std::minstd_rand generator(std::rand());
   std::uniform_int_distribution<unsigned> scanLengthDistribution{1, maxScanLength};

   unsigned insertedCount = initialKeyCount;
   unsigned sampleIndex = 0;
   {
      e.setParam("op", "ycsb_e");
      BTreeCppPerfEventBlock b(e, t, opCount);
      if (!dryRun)
         for (uint64_t completedOps = 0; completedOps < opCount; ++completedOps, ++sampleIndex) {
            if (op_next(e)) {
               if (insertedCount == reasonableMaxKeys) {
                  std::cerr << "exhausted keys for insertion" << std::endl;
                  abort();
               }
               uint8_t* key = (uint8_t*)data[insertedCount].data();
               unsigned int length = data[insertedCount].size();
               t.insert(key, length, payload, payloadSize);
               ++insertedCount;
            } else {
               unsigned scanLength = scanLengthDistribution(generator);
               while (true) {
                  // num_keys for zipf distribution must remain constant to not mess with shuffling permutation.
                  unsigned keyIndex = zipf_next(e, reasonableMaxKeys, zipfParameter, true, true);
                  // COUNTER(zipf_reject_rate_E, keyIndex >= insertedCount, 1 << 10);
                  if (keyIndex < insertedCount) {
                     uint8_t keyBuffer[BTreeNode::maxKVSize];
                     unsigned foundIndex = 0;
                     uint8_t* key = (uint8_t*)data[keyIndex].data();
                     unsigned int keyLen = data[keyIndex].size();
                     auto callback = [&](unsigned keyLen, uint8_t* payload, unsigned loadedPayloadLen) {
                        if (payloadSize != loadedPayloadLen) {
                           throw;
                        }
                        foundIndex += 1;
                        return foundIndex < scanLength;
                     };
                     t.range_lookup(key, keyLen, keyBuffer, callback);
                     break;
                  } else {
                     ++sampleIndex;
                  }
               }
            }
         }
   }
}

void runSortedScan(BTreeCppPerfEvent e,
                   vector<string>& data,
                   unsigned keyCount,
                   unsigned payloadSize,
                   unsigned opCount,
                   unsigned maxScanLength,
                   double zipfParameter,
                   bool dryRun)
{
   if (keyCount <= data.size()) {
      if (!dryRun)
         random_shuffle(data.begin(), data.end());
      data.resize(keyCount);
   } else {
      std::cerr << "not enough keys" << std::endl;
      keyCount = 0;
      opCount = 0;
   }

   uint8_t* payload = makePayload(payloadSize);

   DataStructureWrapper t(isDataInt(e));
   {
      // insert
      e.setParam("op", "sorted_scan_init");
      BTreeCppPerfEventBlock b(e, t, keyCount);
      if (!dryRun)
         for (uint64_t i = 0; i < keyCount; i++) {
            uint8_t* key = (uint8_t*)data[i].data();
            unsigned int length = data[i].size();
            t.insert(key, length, payload, payloadSize);
         }
   }
   uint8_t keyBuffer[BTreeNode::maxKVSize];
   std::minstd_rand generator(std::rand());
   std::uniform_int_distribution<unsigned> scanLengthDistribution{1, maxScanLength};

   t.range_lookup(payload, 0, keyBuffer, [&](unsigned keyLen, uint8_t* payload, unsigned loadedPayloadLen) { return true; });

   {
      e.setParam("op", "sorted_scan");
      BTreeCppPerfEventBlock b(e, t, opCount);
      if (!dryRun)
         for (uint64_t i = 0; i < opCount; i++) {
            unsigned keyIndex = zipf_next(e, keyCount, zipfParameter, false, false);
            assert(keyIndex < data.size());
            unsigned scanLength = scanLengthDistribution(generator);

            unsigned foundIndex = 0;
            uint8_t* key = (uint8_t*)data[keyIndex].data();
            unsigned int keyLen = data[keyIndex].size();
            auto callback = [&](unsigned keyLen, uint8_t* payload, unsigned loadedPayloadLen) {
               if (payloadSize != loadedPayloadLen) {
                  throw;
               }
               foundIndex += 1;
               return foundIndex < scanLength;
            };
            t.range_lookup(key, keyLen, keyBuffer, callback);
         }
   }
}

int main(int argc, char* argv[])
{
   bool dryRun = getenv("DRYRUN");

   vector<string> data;

   unsigned rand_seed = getenv("SEED") ? envu64("SEED") : time(NULL);
   srand(rand_seed);
   unsigned keyCount = envu64("KEY_COUNT");
   if (!getenv("DATA")) {
      std::cerr << "no keyset" << std::endl;
      abort();
   }
   const char* run_id = getenv("RUN_ID");
   if (!run_id) {
      std::cerr << "WARN: no run_id" << std::endl;
      char* timestmap = new char[64];
      sprintf(timestmap, "%lu",
              std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());
      run_id = timestmap;
   }
   std::string keySet = getenv("DATA");
   unsigned payloadSize = envu64("PAYLOAD_SIZE");
   unsigned opCount = envu64("OP_COUNT");
   double zipfParameter = envf64("ZIPF");
   double intDensity = envf64("DENSITY");
   BTreeCppPerfEvent e = makePerfEvent(keySet, false, keyCount);
   e.setParam("payload_size", payloadSize);
   e.setParam("run_id", run_id);
   e.setParam("ycsb_zipf", zipfParameter);
   e.setParam("bin_name", std::string{argv[0]});
   e.setParam("density", intDensity);
   e.setParam("rand_seed", rand_seed);
   unsigned maxScanLength = envu64("SCAN_LENGTH");
   if (maxScanLength == 0) {
      throw;
   }
   e.setParam("ycsb_range_len", maxScanLength);

   if (keySet == "int") {
      std::random_device rd;
      std::mt19937 gen(rd());
      // Create a bernoulli_distribution with the given probability
      std::bernoulli_distribution dist(intDensity);

      // Generate a random boolean value
      bool result = dist(gen);
      unsigned genCount = (envu64("YCSB_VARIANT") == 3 ? keyCount : keyCount + opCount) / intDensity;
      vector<uint32_t> v;
      if (dryRun) {
         data.resize(genCount);
      } else {
         for (uint32_t i = 0; v.size() < genCount; i++)
            v.push_back(i);
         string s;
         s.resize(4);
         for (auto x : v) {
            *(uint32_t*)(s.data()) = __builtin_bswap32(x);
            data.push_back(s);
         }
      }
   } else if (keySet == "rng4") {
      std::random_device rd;
      std::mt19937 gen(rd());

      // Create a bernoulli_distribution with the given probability
      std::uniform_int_distribution dist(uint32_t(0), UINT32_MAX);

      // Generate a random boolean value
      bool result = dist(gen);
      unsigned genCount = envu64("YCSB_VARIANT") == 3 ? keyCount : keyCount + opCount;
      vector<uint32_t> v;
      if (dryRun) {
         data.resize(genCount);
      } else {
         std::set<uint32_t> keys;
         while (keys.size() < genCount)
            keys.insert(dist(gen));
         string s;
         s.resize(4);
         for (uint32_t x : keys) {
            *(uint32_t*)(s.data()) = __builtin_bswap32(x);
            data.push_back(s);
         }
      }
   } else if (keySet == "long1") {
      for (unsigned i = 0; i < keyCount; i++) {
         string s;
         for (unsigned j = 0; j < i; j++)
            s.push_back('A');
         data.push_back(s);
      }
   } else if (keySet == "long2") {
      for (unsigned i = 0; i < keyCount; i++) {
         string s;
         for (unsigned j = 0; j < i; j++)
            s.push_back('A' + random() % 60);
         data.push_back(s);
      }
   } else if (keySet == "partitioned_id") {
      unsigned partitionCount = maxScanLength;
      std::vector<uint32_t> next_id;
      for (unsigned i = 0; i < partitionCount; ++i)
         next_id.push_back(0);

      std::mt19937 gen(rand_seed);
      std::uniform_int_distribution dist(uint32_t(0), uint32_t(partitionCount-1));

      data.reserve(keyCount);
      for (uint32_t i = 0; i < keyCount; i++) {
         uint64_t partition = dist(gen);
         uint64_t id = next_id[partition]++;
         union {
            uint64_t key;
            uint8_t keyBytes[8];
         };
         key = __builtin_bswap64(partition << 32 | id);
         data.emplace_back(keyBytes, keyBytes + 8);
      }
   } else {
      ifstream in(keySet);
      keySet = "file:" + keySet;
      if (dryRun && keySet == "file:data/access")
         data.resize(6625815);
      else if (dryRun && keySet == "file:data/genome")
         data.resize(262084);
      else if (dryRun && keySet == "file:data/urls")
         data.resize(6393703);
      else if (dryRun && keySet == "file:data/urls-short")
         data.resize(6391379);
      else if (dryRun && keySet == "file:data/wiki")
         data.resize(15772029);
      else if (dryRun) {
         std::cerr << "key count unknown for [" << keySet << "]" << std::endl;
         abort();
      } else {
         string line;
         while (getline(in, line)) {
            if (dryRun) {
               data.emplace_back();
            } else {
               if (configName == std::string{"art"})
                  line.push_back(0);
               data.push_back(line);
            }
         }
      }
   }

   for (unsigned i = 0; i < data.size(); ++i) {
      if (data[i].size() + payloadSize > BTreeNode::maxKVSize) {
         std::cerr << "key too long for page size" << std::endl;
         // this forces the key count check to fail and emits nan values.
         data.clear();
         keyCount = 1;
         break;
      }
   }

   switch (envu64("YCSB_VARIANT")) {
      case 3: {
         runYcsbC(e, data, keyCount, payloadSize, opCount, zipfParameter, dryRun);
         break;
      }
      case 4: {
         runYcsbD(e, data, keyCount, payloadSize, opCount, zipfParameter, dryRun);
         break;
      }
      case 401: {
         runSortedInsert(e, data, keyCount, payloadSize, dryRun);
         break;
      }
      case 402: {
         runSortedInsert(e, data, keyCount, payloadSize, dryRun, false);
         break;
      }
      case 5: {
         runYcsbE(e, data, keyCount, payloadSize, opCount, maxScanLength, zipfParameter, dryRun);
         break;
      }
      case 501: {
         runSortedScan(e, data, keyCount, payloadSize, opCount, maxScanLength, zipfParameter, dryRun);
         break;
      }
      default: {
         std::cerr << "bad ycsb variant" << std::endl;
         abort();
      }
   }

   return 0;
}
