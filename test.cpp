#include <csignal>
#include <algorithm>
#include <fstream>
#include <string>
#include "PerfEvent.hpp"
#include "btree2020.hpp"

using namespace std;

void runTest(PerfEvent& e, vector<string>& data)
{
   if (getenv("SHUF"))
      random_shuffle(data.begin(), data.end());
   if (getenv("SORT"))
      sort(data.begin(), data.end());

   BTree t;
   uint64_t count = data.size();
   e.setParam("type", "btr");
   e.setParam("factr", "0");
   e.setParam("base", "0");
   {
      // insert
      e.setParam("op", "insert");
      PerfEventBlock b(e, count);
      for (uint64_t i = 0; i < count; i++) {
         t.insert((uint8_t*)data[i].data(), data[i].size(), reinterpret_cast<uint8_t*>(&i), sizeof(uint64_t));

         //for (uint64_t j=0; j<=i; j+=1) if (!t.lookup((uint8_t*)data[j].data(), data[j].size())) throw;
         //for (uint64_t j=0; j<=i; j++) if (!t.lookup((uint8_t*)data[j].data(), data[j].size()-8)) throw;
      }
   }

   {
      // lookup
      e.setParam("op", "lookup");
      PerfEventBlock b(e, count);
      for (uint64_t i = 0; i < count; i++) {
         unsigned payloadSize;
         uint8_t* payload = t.lookup((uint8_t*)data[i].data(), data[i].size(), payloadSize);
         if (!payload || (payloadSize != sizeof(uint64_t)) || *reinterpret_cast<uint64_t*>(payload) != i)
            throw;
      }
   }

   // prefix lookup
   for (uint64_t i = 0; i < count; i++)
      t.lookup((uint8_t*)data[i].data(), data[i].size() - (data[i].size() / 4));

   {
      for (uint64_t i = 0; i < count; i += 4) // remove some
         if (!t.remove((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count; i++) // lookup all, causes some misses
         if ((i % 4 == 0) == t.lookup((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count / 2 + count / 4; i++) // remove some more
         if ((i % 4 == 0) == t.remove((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count / 2 + count / 4; i++) // insert
         t.insert((uint8_t*)data[i].data(), data[i].size(), reinterpret_cast<uint8_t*>(&i), sizeof(uint64_t));
      for (uint64_t i = 0; i < count; i++) // remove all
         t.remove((uint8_t*)data[i].data(), data[i].size());
      for (uint64_t i = 0; i < count; i++)
         if (t.lookup((uint8_t*)data[i].data(), data[i].size()))
            throw;
   }

   data.clear();
}

int main(int argc, char** argv)
{
   PerfEvent e;

   vector<string> data;

   if (getenv("INT")) {
      vector<uint64_t> v;
      uint64_t n = atof(getenv("INT"));
      for (uint64_t i = 0; i < n; i++)
         v.push_back(i);
      string s;
      s.resize(4);
      for (auto x : v) {
         *(uint32_t*)(s.data()) = __builtin_bswap32(x);
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("LONG1")) {
      uint64_t n = atof(getenv("LONG1"));
      for (unsigned i = 0; i < n; i++) {
         string s;
         for (unsigned j = 0; j < i; j++)
            s.push_back('A');
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("LONG2")) {
      uint64_t n = atof(getenv("LONG2"));
      for (unsigned i = 0; i < n; i++) {
         string s;
         for (unsigned j = 0; j < i; j++)
            s.push_back('A' + random() % 60);
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("FILE")) {
      ifstream in(getenv("FILE"));
      string line;
      while (getline(in, line))
         data.push_back(line);
      runTest(e, data);
   }

   return 0;
}
