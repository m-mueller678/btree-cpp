#include <algorithm>
#include <csignal>
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

   uint64_t total = 0;
   {
      // insert
      e.setParam("op", "hash");
      PerfEventBlock b(e, count * 3);
      for (unsigned i = 0; i < 3; ++i) {
         for (uint64_t i = 0; i < count; i++) {
            total += HashNode::compute_hash((uint8_t*)data[i].data(), data[i].size());
         }
      }
   }
   printf("%lu", total);
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
