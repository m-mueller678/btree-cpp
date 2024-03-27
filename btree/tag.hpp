#pragma once

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

struct TagAndDirty {
  private:
   uint8_t x;
   RangeOpCounter rangeOpCounter;

  public:
   TagAndDirty():x(255){}
   TagAndDirty(Tag t) :TagAndDirty() { set_tag(t); }
   Tag tag() { return static_cast<Tag>(x & 127); }
   void set_tag(Tag t){x = static_cast<uint8_t>(t) | (x & 128);}
   void set_dirty(bool d) { x = x & 127 | static_cast<uint8_t>(d) << 7; }
   bool dirty() { return (x & 128) != 0; }
};


struct RangeOpCounter {
   uint8_t count;
   static constexpr uint8_t MAX_COUNT = 3;

   static std::bernoulli_distribution range_dist;
   static std::bernoulli_distribution point_dist;
   static std::minstd_rand rng;

   RangeOpCounter(uint8_t c = MAX_COUNT / 2) : count(c){};

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

   bool range_op()
   {
      if (!enableAdaptOp) {
         return false;
      }
      if (count < MAX_COUNT) {
         count += (rng() < RANGE_THRESHOLD);
         return count == MAX_COUNT;
      } else {
         return false;
      }
   }

   bool point_op()
   {
      if (!enableAdaptOp) {
         return false;
      }
      if (int8_t(count) > 0) {
         count -= (rng() < POINT_THRESHOLD);
         return count == 0;
      } else {
         return false;
      }
   }

   bool isLowRange()
   {
      if (!enableAdaptOp)
         return true;
      return count <= MAX_COUNT / 2;
   }
};
