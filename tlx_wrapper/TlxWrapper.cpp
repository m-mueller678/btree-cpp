#include <tlx/container/btree.hpp>
#include "TlxWrapper.h"
#include <span>
#include "../btree/tuple.hpp"


uint32_t loadInt(uint8_t const* src){
    union {
        uint32_t i;
        uint8_t b[4];
    }x;
    memcpy(x.b,src,4);
    return x.i;
}

struct TupleIdentityExtractor{
    static Tuple const& get(Tuple* const& t){
        return * t;
    }
};

struct TupleIntCompare{
    bool operator()(Tuple const& lhs,Tuple const& rhs)const{
        return loadInt(lhs.data)< loadInt(rhs.data);
    }
};

struct TupleStringCompare{
    bool operator()(Tuple const& lhs,Tuple const& rhs)const{
        return std::basic_string_view<uint8_t>{lhs.data,lhs.keyLen} < std::basic_string_view<uint8_t>{rhs.data,rhs.keyLen};
    }
};

struct TupleKeyExtractor{
    static std::basic_string_view<uint8_t> get(Tuple* const& t){
        return std::basic_string_view<uint8_t>{t->data,t->keyLen};
    }
};

struct TupleIntKeyExtractor{
    static uint32_t get(Tuple* const& t){
        return loadInt(t->data);
    }
};

struct TlxImpl{
   tlx::BTree<Tuple,Tuple*,TupleIdentityExtractor,TupleStringCompare> strings;
   tlx::BTree<Tuple,Tuple*,TupleIdentityExtractor,TupleIntCompare> ints;
   bool isInt;

    TlxImpl(bool isInt):strings{},
    ints{},
    isInt(isInt){}
};


TlxWrapper::TlxWrapper(bool isInt):impl(new TlxImpl(isInt)){}


uint8_t *TlxWrapper::lookupImpl(uint8_t *key, unsigned int keyLength, unsigned int &payloadSizeOut) {
    static uint8_t EmptyPayload=0;
    Tuple* needle = reinterpret_cast<Tuple*>(alloca(sizeof(Tuple) + keyLength));
    memcpy(needle->data,key,keyLength);
    if(impl->isInt){
        auto it = impl->ints.find(*needle);
        if(it==impl->ints.end()){
            return nullptr;
        }else{
            payloadSizeOut = (*it)->payloadLen;
            return (*it)->payload();
        }
    }else{
        auto it = impl->strings.find(*needle);
        if(it==impl->strings.end()){
            return nullptr;
        }else{
            payloadSizeOut = (*it)->payloadLen;
            return (*it)->payload();
        }
    }
}

void TlxWrapper::insertImpl(uint8_t *key, unsigned int keyLength, uint8_t *payload, unsigned int payloadLength) {
    std::vector<uint8_t> value { payload,payload+payloadLength};
    if(impl->isInt){
        Tuple* tuple=reinterpret_cast<Tuple*>(Tuple::makeTuple(key,keyLength,payload,payloadLength));
        impl->ints.insert(tuple);
    }else{
        Tuple* tuple=reinterpret_cast<Tuple*>(Tuple::makeTuple(key,keyLength,payload,payloadLength));
        impl->strings.insert(tuple);
    }
}

bool TlxWrapper::removeImpl(uint8_t *key, unsigned int keyLength) const {
    abort();
}

void TlxWrapper::range_lookupImpl(uint8_t *key, unsigned int keyLen, uint8_t *keyOut,
                                  const std::function<bool(unsigned int, uint8_t *, unsigned int)> &found_record_cb) {

}


void TlxWrapper::range_lookup_descImpl(uint8_t *key, unsigned int keyLen, uint8_t *keyOut,
                                       const std::function<bool(unsigned int, uint8_t *,
                                                                unsigned int)> &found_record_cb) {
    abort();
}

TlxWrapper::~TlxWrapper() {
    delete impl;
}
