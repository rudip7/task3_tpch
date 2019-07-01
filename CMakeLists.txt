cmake_minimum_required(VERSION 2.6)
 
project(tpch_framework)

# enable c++11
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE "Release")

find_package(OpenMP REQUIRED)
if (OPENMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif() 

# Configure required Boost libraries
set(BOOST_ROOT "" CACHE PATH "Boost build root (useful on Windows)")
option(Boost_USE_STATIC_LIBS
       "Search for static boost libs" OFF)
option(Boost_USE_MULTITHREADED
       "Search for multithreaded boost libs" ON)
option(Boost_USE_STATIC_RUNTIME
       "Search for boost libs linked against static C++ runtime" OFF)
find_package(Boost 1.47.0 REQUIRED filesystem system)

# ensure that dependant libraries not explicitly specified here
# are found by the linker:
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})
 
#Bring the headers into the project
include_directories(include)
  
FILE(GLOB_RECURSE INC_ALL "include/*.hpp")

#However, the file(GLOB...) allows for wildcard additions:
file(GLOB SOURCES "src/*.cpp")
 
add_library(tpch_framework ${SOURCES})
add_executable(framework main.cpp ${INC_ALL})
target_link_libraries(framework tpch_framework)
#target_link_libraries(framework stdc++fs)
target_link_libraries(framework ${LIBS})
