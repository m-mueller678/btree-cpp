#include <fcntl.h>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <csignal>
#include <exception>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <span>
#include <thread>
#include <vector>

#include <immintrin.h>
#include <libaio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include "../libbtree.hpp"

#define TBB_SUPPRESS_DEPRECATED_MESSAGES 1

#include <tbb/parallel_for.h>
#include <tbb/task_scheduler_init.h>

#include "../PerfEvent.hpp"
#include "exception_hack.hpp"

__thread uint16_t workerThreadId = 0;
__thread int32_t tpcchistorycounter = 0;

#include "tpcc/TPCCWorkload.hpp"

using namespace std;

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef u64 PID;

static const int16_t maxWorkerThreads = 256;
atomic<int16_t> workerThreadIdCounter(0);

void initWorkerId()
{
   if (workerThreadId)
      return;
   workerThreadId = workerThreadIdCounter++;
   assert(workerThreadId < maxWorkerThreads);
}

#define die(msg)          \
   do {                   \
      perror(msg);        \
      exit(EXIT_FAILURE); \
   } while (0)

uint64_t rdtsc()
{
   uint32_t hi, lo;
   __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
   return static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
}

void* allocHuge(size_t size)
{
   void* p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_HUGEPAGE);
   return p;
}

void yield(u64 counter)
{
   _mm_pause();
}

struct PageState {
   atomic<u64> stateAndVersion;

   static const u64 Unlocked = 0;
   static const u64 MaxShared = 252;
   static const u64 Locked = 253;
   static const u64 Marked = 254;
   static const u64 Evicted = 255;

   PageState() {}

   void init() { stateAndVersion.store(sameVersion(0, Evicted), std::memory_order_release); }

   static inline u64 sameVersion(u64 oldStateAndVersion, u64 newState) { return ((oldStateAndVersion << 8) >> 8) | newState << 56; }

   static inline u64 nextVersion(u64 oldStateAndVersion, u64 newState) { return (((oldStateAndVersion << 8) >> 8) + 1) | newState << 56; }

   bool tryLockX(u64 oldStateAndVersion)
   {
      return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Locked));
   }

   void unlockX()
   {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
   }

   void unlockXEvicted()
   {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), Evicted), std::memory_order_release);
   }

   void downgradeLock()
   {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), 1), std::memory_order_release);
   }

   bool tryLockS(u64 oldStateAndVersion)
   {
      u64 s = getState(oldStateAndVersion);
      if (s < MaxShared)
         return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, s + 1));
      if (s == Marked)
         return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, 1));
      return false;
   }

   void unlockS()
   {
      while (true) {
         u64 oldStateAndVersion = stateAndVersion.load();
         u64 state = getState(oldStateAndVersion);
         assert(state > 0 && state <= MaxShared);
         if (stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, state - 1)))
            return;
      }
   }

   bool tryMark(u64 oldStateAndVersion)
   {
      assert(getState(oldStateAndVersion) == Unlocked);
      return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Marked));
   }

   static u64 getState(u64 v) { return v >> 56; };

   u64 getState() { return getState(stateAndVersion.load()); }

   void operator=(PageState&) = delete;
};

static const u64 pageSize = 4096;

struct OLCRestartException {
};

u64 envOr(const char* env, u64 value)
{
   if (getenv(env))
      return atoi(getenv(env));
   return value;
}

static unsigned min(unsigned a, unsigned b)
{
   return a < b ? a : b;
}

template <class T>
static T loadUnaligned(void* p)
{
   T x;
   memcpy(&x, p, sizeof(T));
   return x;
}

static unsigned btreeslotcounter = 0;

typedef u64 KeyType;

template <class Record>
struct vmcacheAdapter {
   BTree* tree;

  public:
   vmcacheAdapter() { tree = btree_new(); }

   void scan(const typename Record::Key& key,
             const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb,
             std::function<void()> reset_if_scan_failed_cb)
   {
      static u8 k[Record::maxFoldLength()];
      static std::function<bool(const typename Record::Key&, const Record&)> const* found_record_cb_ptr;
      u16 l = Record::foldKey(k, key);
      static u8 kk[Record::maxFoldLength()];
      found_record_cb_ptr = &found_record_cb;

      btree_scan_asc(tree, k, l, kk, [](uint8_t const* payload) {
         typename Record::Key typedKey;
         Record::unfoldKey(kk, typedKey);
         return (*found_record_cb_ptr)(typedKey, *reinterpret_cast<const Record*>(payload));
      });
   }

   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb,
                 std::function<void()> reset_if_scan_failed_cb)
   {
      // TODO
      /*u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      u8 kk[Record::maxFoldLength()];
      bool first = true;

      tree.scanDesc({k, l}, [&](BTreeNode &node, unsigned slot, bool exactMatch) {
          if (first) { // XXX: hack
              first = false;
              if (!exactMatch)
                  return true;
          }
          memcpy(kk, node.getPrefix(), node.prefixLen);
          memcpy(kk + node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
          typename Record::Key typedKey;
          Record::unfoldKey(kk, typedKey);
          return found_record_cb(typedKey, *reinterpret_cast<const Record *>(node.getPayload(slot).data()));
      });
       */
   }

   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record)
   {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      btree_insert(tree, k, l, (u8*)(&record), sizeof(Record));
   }

   // -------------------------------------------------------------------------------------
   template <class Fn>
   void lookup1(const typename Record::Key& key, Fn fn)
   {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      u32 len_out;
      u8* value_ptr = btree_lookup(tree, k, l, &len_out);
      assert(value_ptr);
      fn(*reinterpret_cast<const Record*>(value_ptr));
   }

   // -------------------------------------------------------------------------------------
   template <class Fn>
   void update1(const typename Record::Key& key, Fn fn)
   {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      u32 len_out;
      u8* value_ptr = btree_lookup(tree, k, l, &len_out);
      if (value_ptr) {
         fn(*reinterpret_cast<Record*>(value_ptr));
      }
   }

   // -------------------------------------------------------------------------------------
   // Returns false if the record was not found
   bool erase(const typename Record::Key& key)
   {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      return btree_remove(tree, k, l);
   }

   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f)
   {
      Field value;
      lookup1(key, [&](const Record& r) { value = r.*f; });
      return value;
   }

   u64 count()
   {
      static u8 kk[Record::maxFoldLength()];
      static u64 cnt;
      cnt = 0;
      btree_scan_asc(tree, (u8 const*)&cnt, 0, kk, [](u8 const* payload) {
         cnt++;
         return true;
      });
      return cnt;
   }

   u64 countw(Integer w_id)
   {
      static u8 k[sizeof(Integer)];
      static u64 cnt;
      static u8 kk[Record::maxFoldLength()];

      fold(k, w_id);
      cnt = 0;

      btree_scan_asc(tree, k, sizeof(Integer), kk, []() {
         if (memcmp(k, kk, sizeof(Integer)) != 0)
            return false;
         cnt++;
         return true;
      });
      return cnt;
   }

   u64 countParallel(Integer warehouseCount)
   {
      atomic<u64> count(0);
      tbb::parallel_for(tbb::blocked_range<Integer>(1, warehouseCount + 1), [&](const tbb::blocked_range<Integer>& range) {
         initWorkerId();
         for (Integer w_id = range.begin(); w_id < range.end(); w_id++) {
            count += countw(w_id);
         }
      });
      return count.load();
   }
};

int main(int argc, char** argv)
{
   exception_hack::init_phdr_cache();

   PerfEvent e;
   if (argc != 3) {
      cout << "usage: " << argv[0] << " <threads> <datasize>" << endl;
      exit(1);
   }
   unsigned nthreads = 1;
   u64 n = atof(argv[2]);
   tbb::task_scheduler_init init(nthreads);
   u64 runForSec = envOr("RUNFOR", 30);
   bool isYcsb = envOr("YCSB", 0);

   u64 statDiff = 1e8;
   atomic<u64> txProgress(0);
   atomic<bool> keepRunning(true);
   auto systemName = "RustBTree";

   // TPC-C
   Integer warehouseCount = n;

   vmcacheAdapter<warehouse_t> warehouse;
   vmcacheAdapter<district_t> district;
   vmcacheAdapter<customer_t> customer;
   vmcacheAdapter<customer_wdl_t> customerwdl;
   vmcacheAdapter<history_t> history;
   vmcacheAdapter<neworder_t> neworder;
   vmcacheAdapter<order_t> order;
   vmcacheAdapter<order_wdc_t> order_wdc;
   vmcacheAdapter<orderline_t> orderline;
   vmcacheAdapter<item_t> item;
   vmcacheAdapter<stock_t> stock;

   TPCCWorkload<vmcacheAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock, true,
                                     warehouseCount, true);

   {
      // PerfEventBlock b(e, warehouseCount*644446ull);
      tpcc.loadItem();
      tpcc.loadWarehouse();
      tbb::parallel_for(tbb::blocked_range<Integer>(1, warehouseCount + 1), [&](const tbb::blocked_range<Integer>& range) {
         initWorkerId();
         for (Integer w_id = range.begin(); w_id < range.end(); w_id++) {
            tpcc.loadStock(w_id);
            tpcc.loadDistrinct(w_id);
            for (Integer d_id = 1; d_id <= 10; d_id++) {
               tpcc.loadCustomer(w_id, d_id);
               tpcc.loadOrders(w_id, d_id);
            }
         }
      });
   }
   /*
   {
      assert(warehouse.count() == warehouseCount);
      assert(district.count() == warehouseCount*10);
      cerr << "space: " << (bm.allocCount.load()*pageSize)/(float)bm.gb << " GB ";
      cerr << "ol: " << orderline.countParallel(warehouseCount) << " ";
      u64 o = order.countParallel(warehouseCount);
      cerr << "o: " << o << " ";
      assert(order_wdc.countParallel(warehouseCount) == o);
      cerr << "s: " << stock.countParallel(warehouseCount) << " ";
      u64 c = customer.countParallel(warehouseCount);
      cerr << "c: " << c << " ";
      assert(customerwdl.countParallel(warehouseCount) == c);
      cerr << "n: " << neworder.countParallel(warehouseCount) << endl;
   }
   */

   std::cerr << "setup complete" << std::endl;
   vector<thread> threads;

   for (unsigned worker = 0; worker < nthreads; worker++) {
      threads.emplace_back([&, worker]() {
         workerThreadId = worker;
         u64 cnt = 0;
         u64 start = rdtsc();
         while (keepRunning.load()) {
            int w_id = tpcc.urand(1, warehouseCount);  // wh crossing
            tpcc.tx(w_id);
            cnt++;
            u64 stop = rdtsc();
            if ((stop - start) > statDiff) {
               txProgress += cnt;
               start = stop;
               cnt = 0;
            }
         }
         txProgress += cnt;
      });
   }

   sleep(runForSec);
   keepRunning = false;
   for (auto& t : threads)
      t.join();

   print_tpcc_result(runForSec, txProgress, warehouseCount);

   return 0;
}

/*
 - better yield
 - merge inner
 - disk free space management
 - writing: dirty, async background writer?
 - OLC reads: go through unsigned vs u16
 */
