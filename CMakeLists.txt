cmake_minimum_required(VERSION 3.15)
project(udp_p2p CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(test_protocol tests/test_protocol.cpp)