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

constexpr unsigned ZIPF_GEN_SIZE = 1 << 23;
constexpr unsigned OP_GEN_SIZE = ZIPF_GEN_SIZE << 2;
constexpr double ZIPF_PARAMETER = 1.5;
constexpr unsigned MAX_SCAN_LENGTH = 20;

uint64_t envu64(const char* env)
{
   if (getenv(env))
      return atof(getenv(env));
   std::cerr << "missing env " << env << std::endl;
   abort();
}

unsigned* makeZipfIndexArray(unsigned genSize, unsigned keyCount)
{
   unsigned* zipfIndices = new unsigned[genSize];
   if (keyCount > 0) {
      ZipfGenerator* generator = zipf_init_generator(keyCount, ZIPF_PARAMETER);
      zipf_generate(generator, zipfIndices, genSize);
   }
   return zipfIndices;
}

bool* makeOpArray(unsigned genSize, double trueRate)
{
   bool* op = new bool[genSize];
   unsigned trueCount = lround(genSize * trueRate);
   for (unsigned i = 0; i < genSize; ++i)
      op[i] = i < trueCount;
   random_shuffle(op, op + genSize);
   return op;
}

uint8_t* makePayload(unsigned len)
{
   uint8_t* payload = new uint8_t[len];
   memset(payload, 42, len);
   return payload;
}

void runYcsbC(BTreeCppPerfEvent e, vector<string>& data, unsigned keyCount, unsigned payloadSize, unsigned opCount)
{
   if (keyCount <= data.size()) {
      random_shuffle(data.begin(), data.end());
      data.resize(keyCount);
   } else {
      std::cerr << "not enough keys" << std::endl;
      keyCount = 0;
      opCount = 0;
   }

   uint8_t* payload = makePayload(payloadSize);
   unsigned* zipf_indices = makeZipfIndexArray(ZIPF_GEN_SIZE, keyCount);

   DataStructureWrapper t;
   {
      // insert
      e.setParam("op", "ycsb_c_init");
      BTreeCppPerfEventBlock b(e, keyCount);
      for (uint64_t i = 0; i < keyCount; i++) {
         uint8_t* key = (uint8_t*)data[i].data();
         unsigned int length = data[i].size();
         t.insert(key, length, payload, payloadSize);
      }
   }

   {
      e.setParam("op", "ycsb_c");
      BTreeCppPerfEventBlock b(e, opCount);
      for (uint64_t i = 0; i < opCount; i++) {
         unsigned keyIndex = zipf_indices[i % ZIPF_GEN_SIZE] - 1;
         assert(keyIndex < data.size());
         unsigned payloadSizeOut;
         uint8_t* key = (uint8_t*)data[keyIndex].data();
         unsigned long length = data[keyIndex].size();
         uint8_t* payload = t.lookup(key, length, payloadSizeOut);
         if (!payload || (payloadSize != payloadSizeOut) || *payload != 42)
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

void runYcsbD(BTreeCppPerfEvent e, vector<string>& data, unsigned avgKeyCount, unsigned payloadSize, unsigned opCount)
{
   unsigned initialKeyCount = 0;
   if (!computeInitialKeyCount(avgKeyCount, data.size(), opCount, initialKeyCount)) {
      opCount = 0;
      initialKeyCount = 0;
      data.resize(0);
   }

   random_shuffle(data.begin(), data.end());
   uint8_t* payload = makePayload(payloadSize);
   unsigned* zipf_indices = makeZipfIndexArray(ZIPF_GEN_SIZE, data.size());
   bool* op_array = makeOpArray(OP_GEN_SIZE, 0.05);

   DataStructureWrapper t;
   {
      // insert
      e.setParam("op", "ycsb_d_init");
      BTreeCppPerfEventBlock b(e, initialKeyCount);
      for (uint64_t i = 0; i < initialKeyCount; i++) {
         uint8_t* key = (uint8_t*)data[i].data();
         unsigned int length = data[i].size();
         t.insert(key, length, payload, payloadSize);
      }
   }

   unsigned insertedCount = initialKeyCount;
   unsigned sampleIndex = 0;
   {
      e.setParam("op", "ycsb_d");
      BTreeCppPerfEventBlock b(e, opCount);
      for (uint64_t completedOps = 0; completedOps < opCount; ++completedOps, ++sampleIndex) {
         if (op_array[sampleIndex % OP_GEN_SIZE]) {
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
               unsigned zipfSample = zipf_indices[sampleIndex % ZIPF_GEN_SIZE];
               if (zipfSample < insertedCount) {
                  unsigned keyIndex = insertedCount - zipfSample;
                  unsigned payloadSizeOut;
                  uint8_t* key = (uint8_t*)data[keyIndex].data();
                  unsigned long length = data[keyIndex].size();
                  uint8_t* payload = t.lookup(key, length, payloadSizeOut);
                  if (!payload || (payloadSize != payloadSizeOut) || *payload != 42)
                     throw;
                  break;
               } else {
                  ++sampleIndex;
               }
            }
         }
      }
   }
}

void runYcsbE(BTreeCppPerfEvent e, vector<string>& data, unsigned avgKeyCount, unsigned payloadSize, unsigned opCount)
{
   unsigned initialKeyCount = 0;
   if (!computeInitialKeyCount(avgKeyCount, data.size(), opCount, initialKeyCount)) {
      opCount = 0;
      initialKeyCount = 0;
      data.resize(0);
   }

   random_shuffle(data.begin(), data.end());
   uint8_t* payload = makePayload(payloadSize);
   unsigned* zipf_indices = makeZipfIndexArray(ZIPF_GEN_SIZE, data.size());
   bool* op_array = makeOpArray(OP_GEN_SIZE, 0.05);

   if (data.size() > 0) {  // permute zipf indices
      unsigned* permutation = new unsigned[data.size()];
      for (unsigned i = 0; i < data.size(); ++i) {
         permutation[i] = i;
      }
      random_shuffle(permutation, permutation + data.size());
      for (unsigned i = 0; i < ZIPF_GEN_SIZE; ++i) {
         zipf_indices[i] = permutation[zipf_indices[i] - 1];
      }
      delete[] permutation;
   }
   DataStructureWrapper t;
   {
      // insert
      e.setParam("op", "ycsb_e_init");
      BTreeCppPerfEventBlock b(e, initialKeyCount);
      for (uint64_t i = 0; i < initialKeyCount; i++) {
         uint8_t* key = (uint8_t*)data[i].data();
         unsigned int length = data[i].size();
         t.insert(key, length, payload, payloadSize);
      }
   }

   std::minstd_rand generator(0xabcdef42);
   std::uniform_int_distribution<unsigned> scanLengthDistribution{1, MAX_SCAN_LENGTH};

   unsigned insertedCount = initialKeyCount;
   unsigned sampleIndex = 0;
   {
      e.setParam("op", "ycsb_e");
      BTreeCppPerfEventBlock b(e, opCount);
      for (uint64_t completedOps = 0; completedOps < opCount; ++completedOps, ++sampleIndex) {
         if (op_array[sampleIndex % OP_GEN_SIZE]) {
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
               unsigned keyIndex = zipf_indices[sampleIndex % ZIPF_GEN_SIZE];
               COUNTER(zipf_reject_rate, !(keyIndex < insertedCount), 1 << 17);
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

int main()
{
   vector<string> data;

   unsigned keyCount = envu64("KEY_COUNT");
   if (!getenv("DATA")) {
      std::cerr << "no keyset" << std::endl;
      abort();
   }
   std::string keySet = getenv("DATA");
   unsigned payloadSize = envu64("PAYLOAD_SIZE");
   unsigned opCount = envu64("OP_COUNT");
   BTreeCppPerfEvent e = makePerfEvent(keySet, false, data.size());
   e.setParam("payload_size", payloadSize);
   e.setParam("run_id", envu64("RUN_ID"));
   e.setParam("ycsb_zipf", ZIPF_PARAMETER);
   e.setParam("ycsb_range_len", MAX_SCAN_LENGTH);

   if (keySet == "int") {
      unsigned genCount = envu64("YCSB_VARIANT") == 3 ? keyCount : keyCount + opCount;
      vector<uint32_t> v;
      for (uint32_t i = 0; i < genCount; i++)
         v.push_back(i);
      string s;
      s.resize(4);
      for (auto x : v) {
         *(uint32_t*)(s.data()) = __builtin_bswap32(x);
         data.push_back(s);
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
      string line;
      while (getline(in, line))
         data.push_back(line);
   }
   switch (envu64("YCSB_VARIANT")) {
      case 3: {
         runYcsbC(e, data, keyCount, payloadSize, opCount);
         break;
      }
      case 4: {
         runYcsbD(e, data, keyCount, payloadSize, opCount);
         break;
      }
      case 5: {
         runYcsbE(e, data, keyCount, payloadSize, opCount);
         break;
      }
      default: {
         std::cerr << "bad ycsb variant" << std::endl;
         abort();
      }
   }

   return 0;
}
