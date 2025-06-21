# cmake_minimum_required(VERSION 3.1..3.5)
cmake_minimum_required(VERSION 3.5 FATAL_ERROR)
cmake_policy(SET CMP0057 NEW)
project(cpp_datetime_parser LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
find_package(pybind11 REQUIRED)

pybind11_add_module(cpp_datetime_parser src/cpp_datetime_parser.cpp)

