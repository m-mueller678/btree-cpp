cmake_minimum_required(VERSION 2.8.12)
project(tlx_wrapper)

set(CMAKE_CXX_STANDARD 20)

add_subdirectory("tlx")

add_library(TlxWrapper TlxWrapper.cpp
        TlxWrapper.h)

target_link_libraries(TlxWrapper tlx)
target_include_directories(TlxWrapper PRIVATE "${CMAKE_SOURCE_DIR}/tlx")