cmake_minimum_required(VERSION 3.15)
project(udp_p2p CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# Settings shared by every program in this project
add_library(common INTERFACE)
target_include_directories(common INTERFACE src tests)
target_link_libraries(common INTERFACE Threads::Threads)
if(WIN32)
  target_link_libraries(common INTERFACE ws2_32)
endif()

add_executable(test_protocol tests/test_protocol.cpp)
target_link_libraries(test_protocol common)

add_executable(test_net tests/test_net.cpp)
target_link_libraries(test_net common)
add_executable(test_reliable tests/test_reliable.cpp)
target_link_libraries(test_reliable common)
add_executable(test_membership tests/test_membership.cpp)
target_link_libraries(test_membership common)
add_executable(test_chat tests/test_chat.cpp)
target_link_libraries(test_chat common)

# The real program
add_executable(peer src/peer.cpp)
target_link_libraries(peer common)