cmake_minimum_required(VERSION 3.5 FATAL_ERROR)
cmake_policy(SET CMP0057 NEW)
# cmake_minimum_required(VERSION 3.1..3.5)
project(cpp_index_call LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
find_package(pybind11 REQUIRED)

# Use the actual source file name here
pybind11_add_module(cpp_index_call src/cpp_index_call.cpp)

