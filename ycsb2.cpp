#include <algorithm>
#include <csignal>
#include <fstream>
#include <random>
#include <string>
#include "btree/BtreeCppPerfEvent.hpp"
#include "btree/btree2020.hpp"

using namespace std;

extern "C" {
struct ZipfGenerator;
ZipfGenerator* zipf_init_generator(uint32_t, double);
void zipf_generate(ZipfGenerator*, uint32_t*, uint32_t);
}

static ZipfGenerator* ZIPF_GENERATOR = nullptr;

unsigned zipf_next(BTreeCppPerfEvent& e)
{
   constexpr unsigned GEN_SIZE = 1 << 25;
   static unsigned ARRAY[GEN_SIZE];
   static unsigned index = GEN_SIZE - 1;

   index += 1;
   if (index == GEN_SIZE)
      index = 0;
   if (index == 0) {
      e.disableCounters();
      zipf_generate(ZIPF_GENERATOR, ARRAY, GEN_SIZE);
      e.enableCounters();
   }
   return ARRAY[index];
}

bool op_next(BTreeCppPerfEvent& e)
{
   constexpr unsigned GEN_SIZE = 5 * (1 << 22);
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

void runYcsbC(BTreeCppPerfEvent e, vector<string>& data, unsigned keyCount, unsigned payloadSize, unsigned opCount, double zipfParameter, bool dryRun)
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
   ZIPF_GENERATOR = zipf_init_generator(keyCount, zipfParameter);

   DataStructureWrapper t;
   {
      // insert
      e.setParam("op", "ycsb_c_init");
      BTreeCppPerfEventBlock b(e, keyCount);
      if (!dryRun)
         for (uint64_t i = 0; i < keyCount; i++) {
            uint8_t* key = (uint8_t*)data[i].data();
            unsigned int length = data[i].size();
            t.insert(key, length, payload, payloadSize);
         }
   }

   {
      e.setParam("op", "ycsb_c");
      BTreeCppPerfEventBlock b(e, opCount);
      if (!dryRun)
         for (uint64_t i = 0; i < opCount; i++) {
            unsigned keyIndex = zipf_next(e);
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

bool computeInitialKeyCount(unsigned avgKeyCount, unsigned availableKeyCount, unsigned opCount, unsigned& initialKeyCount)
{
   bool configValid = false;
   initialKeyCount = 0;
   if (avgKeyCount > opCount / 40) {
      initialKeyCount = avgKeyCount - opCount / 40;
      unsigned expectedInsertions = opCount / 20;
      if (initialKeyCount + expectedInsertions * 2 <= availableKeyCount) {
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
   if (!computeInitialKeyCount(avgKeyCount, data.size(), opCount, initialKeyCount)) {
      opCount = 0;
      initialKeyCount = 0;
      data.resize(0);
   }

   if (!dryRun)
      random_shuffle(data.begin(), data.end());
   uint8_t* payload = makePayload(payloadSize);
   ZIPF_GENERATOR = zipf_init_generator(data.size(), zipfParameter);

   DataStructureWrapper t;
   {
      // insert
      e.setParam("op", "ycsb_d_init");
      BTreeCppPerfEventBlock b(e, initialKeyCount);
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
      BTreeCppPerfEventBlock b(e, opCount);
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
               while (true) {
                  unsigned zipfSample = zipf_next(e);
                  if (zipfSample < insertedCount) {
                     unsigned keyIndex = insertedCount - 1 - zipfSample;
                     unsigned payloadSizeOut;
                     uint8_t* key = (uint8_t*)data[keyIndex].data();
                     unsigned long length = data[keyIndex].size();
                     uint8_t* payload = t.lookup(key, length, payloadSizeOut);
                     if (!payload || (payloadSize != payloadSizeOut) || (payloadSize > 0 && *payload != 42))
                        throw;
                     break;
                  }
               }
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
   if (!computeInitialKeyCount(avgKeyCount, data.size(), opCount, initialKeyCount)) {
      opCount = 0;
      initialKeyCount = 0;
      data.resize(0);
   }

   if (!dryRun)
      random_shuffle(data.begin(), data.end());
   uint8_t* payload = makePayload(payloadSize);
   ZIPF_GENERATOR = zipf_init_generator(data.size(), zipfParameter);
   // TODO zipf permutation so not all indices are among first insertions
   abort();

   if (data.size() > 0) {  // permute zipf indices
      unsigned* permutation = new unsigned[data.size()];
      for (unsigned i = 0; i < data.size(); ++i) {
         permutation[i] = i;
      }
      random_shuffle(permutation, permutation + data.size());
      delete[] permutation;
   }
   DataStructureWrapper t;
   {
      // insert
      e.setParam("op", "ycsb_e_init");
      BTreeCppPerfEventBlock b(e, initialKeyCount);
      if (!dryRun)
         for (uint64_t i = 0; i < initialKeyCount; i++) {
            uint8_t* key = (uint8_t*)data[i].data();
            unsigned int length = data[i].size();
            t.insert(key, length, payload, payloadSize);
         }
   }

   std::minstd_rand generator(0xabcdef42);
   std::uniform_int_distribution<unsigned> scanLengthDistribution{1, maxScanLength};

   unsigned insertedCount = initialKeyCount;
   unsigned sampleIndex = 0;
   {
      e.setParam("op", "ycsb_e");
      BTreeCppPerfEventBlock b(e, opCount);
      if (!dryRun)
         for (uint64_t completedOps = 0; completedOps < opCount; ++completedOps, ++sampleIndex) {
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
               unsigned scanLength = scanLengthDistribution(generator);
               while (true) {
                  unsigned keyIndex = zipf_next(e);
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

int main(int argc, char* argv[])
{
   bool dryRun = getenv("DRYRUN");

   vector<string> data;

   unsigned keyCount = envu64("KEY_COUNT");
   if (!getenv("DATA")) {
      std::cerr << "no keyset" << std::endl;
      abort();
   }
   std::string keySet = getenv("DATA");
   unsigned payloadSize = envu64("PAYLOAD_SIZE");
   unsigned opCount = envu64("OP_COUNT");
   double zipfParameter = envf64("ZIPF");
   BTreeCppPerfEvent e = makePerfEvent(keySet, false, keyCount);
   e.setParam("payload_size", payloadSize);
   e.setParam("run_id", envu64("RUN_ID"));
   e.setParam("ycsb_zipf", zipfParameter);
   e.setParam("bin_name", std::string{argv[0]});
   unsigned maxScanLength = envu64("SCAN_LENGTH");
   e.setParam("ycsb_range_len", maxScanLength);

   if (keySet == "int") {
      unsigned genCount = envu64("YCSB_VARIANT") == 3 ? keyCount : keyCount + opCount;
      vector<uint32_t> v;
      if (dryRun) {
         data.resize(genCount);
      } else {
         for (uint32_t i = 0; i < genCount; i++)
            v.push_back(i);
         string s;
         s.resize(4);
         for (auto x : v) {
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
   } else {
      ifstream in(keySet);
      keySet = "file:" + keySet;
      if (dryRun && keySet == "file:data/access")
         data.resize(6625815);
      else if (dryRun && keySet == "file:data/genome")
         data.resize(262084);
      else if (dryRun && keySet == "file:data/urls")
         data.resize(6393703);
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
      case 5: {
         runYcsbE(e, data, keyCount, payloadSize, opCount, maxScanLength, zipfParameter, dryRun);
         break;
      }
      default: {
         std::cerr << "bad ycsb variant" << std::endl;
         abort();
      }
   }

   return 0;
}
