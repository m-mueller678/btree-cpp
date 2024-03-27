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

struct TagAndDirty {
  private:
   uint8_t x;

  public:
   TagAndDirty():x(255){}
   TagAndDirty(Tag t) :TagAndDirty() { set_tag(t); }
   Tag tag() { return static_cast<Tag>(x & 127); }
   void set_tag(Tag t){x = static_cast<uint8_t>(t) | (x & 128);}
   void set_dirty(bool d) { x = x & 127 | static_cast<uint8_t>(d) << 7; }
   bool dirty() { return (x & 128) != 0; }
};
