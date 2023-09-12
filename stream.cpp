// this should not actually stream data from memory, but renaming is too much effort

#include <algorithm>
#include <iostream>
#include "btree/BtreeCppPerfEvent.hpp"
#include <random>

uint64_t envu64(const char* env)
{
    if (getenv(env))
        return strtod(getenv(env), nullptr);
    std::cerr << "missing env " << env << std::endl;
    abort();
}

int main(){
    auto count= envu64("SIZE")/8;
    auto scale=envu64("SCALE");

    uint64_t* permutation = new uint64_t [count];
    void** data = new void* [count];
    for(uint64_t i=0;i<count;++i){
        permutation[i]=i;
    }
    std::random_shuffle(permutation,permutation+count);
    for(uint64_t i=0;i<count;++i){
        data[permutation[i]] = data + permutation[(i+1)%count];
    };
    for(uint64_t i=0;i<count;++i){
        //std::cout<<permutation[i]<<std::endl;
    }
    delete[] permutation;

    BTreeCppPerfEvent e;
    e.setParam("size", count*8);
    void** p=data;
    {
        BTreeCppPerfEventBlock b(e, scale);
        for(unsigned i=0;i<scale;++i){
            p=reinterpret_cast<void**>(*p);
            //std::cout<<(p-data)<<std::endl;
        }
    }
    std::cerr<<(p-data)<<std::endl;
}