#pragma once

#include<atomic>

enum class Tag : uint8_t {
   Leaf = 0,
   Inner = 1,
   Dense = 2,
   Hash = 3,
   Head4 = 4,
   Head8 = 5,
   Dense2 = 6,
   _last = 6,
};

constexpr unsigned TAG_END = unsigned(Tag::_last) + 1;
const char* tag_name(Tag tag);


struct RangeOpCounter {
   std::atomic<uint8_t> count;
   static constexpr uint8_t MAX_COUNT = 3;

   thread_local static std::bernoulli_distribution range_dist;
   thread_local static std::bernoulli_distribution point_dist;
   thread_local static std::minstd_rand rng;

   RangeOpCounter(uint8_t c = MAX_COUNT / 2) : count(c){};

   RangeOpCounter(RangeOpCounter const& other):count(other.count.load(std::memory_order_relaxed)){}
   RangeOpCounter& operator=(RangeOpCounter const& other){
      count.store(other.count.load(std::memory_order_relaxed),std::memory_order_relaxed);
   }

   void setGoodHeads()
   {
      if (!enableAdaptOp)
         return;
      count = 255;
   }

   void setBadHeads(uint8_t previous = MAX_COUNT / 2)
   {
      if (!enableAdaptOp)
         return;
      if (previous == 255) {
         count = MAX_COUNT / 2;
      } else {
         count = previous;
      }
   }

   static constexpr uint32_t RANGE_THRESHOLD = (rng.max() + 1) * 0.15;
   static constexpr uint32_t POINT_THRESHOLD = (rng.max() + 1) * 0.05;

   void range_op()
   {
      if (!enableAdaptOp) {
         return;
      }
      bool should_inc=false;
      while (true){
         auto c = count.load(std::memory_order_relaxed);
         if (count < MAX_COUNT) {
            if(!should_inc){
               should_inc = rng() < RANGE_THRESHOLD;
            }
            if(!should_inc){ break;}
            if (should_inc){
               if (count.compare_exchange_weak(c,c+1,std::memory_order_relaxed,std::memory_order_relaxed)){
                  break;
               }else{
                  continue;
               }
            }
         } else {
            break;
         }
      }


   }

   void point_op()
   {
      if (!enableAdaptOp) {
         return;
      }
      bool should_dec=false;
      while (true){
         auto c = count.load(std::memory_order_relaxed);
         if (std::int8_t(count) > 0) {
            if(!should_dec){
               should_dec = rng() < POINT_THRESHOLD;
            }
            if(!should_dec){ break;}
            if (should_dec){
               if (count.compare_exchange_weak(c,c+1,std::memory_order_relaxed,std::memory_order_relaxed)){
                  break;
               }else{
                  continue;
               }
            }
         } else {
            break;
         }
      }
   }

   bool isLowRange()
   {
      if (!enableAdaptOp)
         return true;
      return count.load(std::memory_order_relaxed) <= MAX_COUNT / 2;
   }
};


struct TagAndDirty {
  private:
   uint8_t x;
  public:
   RangeOpCounter rangeOpCounter;

   TagAndDirty():x(255),rangeOpCounter(){}
   TagAndDirty(Tag t,RangeOpCounter roc) :x(255), rangeOpCounter(std::move(roc)) { set_tag(t); }
   Tag tag() { return static_cast<Tag>(x & 127); }
   void set_tag(Tag t){x = static_cast<uint8_t>(t) | (x & 128);}
   void set_dirty(bool d) { x = x & 127 | static_cast<uint8_t>(d) << 7; }
   bool dirty() { return (x & 128) != 0; }
};


