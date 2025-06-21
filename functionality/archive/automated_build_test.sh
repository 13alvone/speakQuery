#!/bin/sh

cd ~/test_project_root/

echo "\n# Showing Project Root Layout"
echo "[i] BASH COMMAND: ls -lart"
ls -lart

echo "\n# Showing Project Root Functionality Directory Layout"
echo "[i] BASH COMMAND: ls -lart functionality/"
ls -lart functionality/

echo "\n# Showing CPP Datetime Parser Directory Layout"
echo "[i] BASH COMMAND: ls -lart functionality/cpp_datetime_parser/"
ls -lart functionality/cpp_datetime_parser/
echo "[i] BASH COMMAND: ls -lart functionality/cpp_datetime_parser/src/"
ls -lart functionality/cpp_datetime_parser/src/

echo "\n# Showing CPP Datetime Parser CMakeLists.txt Layout"
echo "[i] BASH COMMAND: cat functionality/cpp_datetime_parser/CMakeLists.txt"
cat functionality/cpp_datetime_parser/CMakeLists.txt

echo "\n# Prove that the tail of the cpp_datetime_parser.cpp calls correct cpp module name"
echo "[i] BASH COMMAND: tail functionality/cpp_datetime_parser/src/cpp_datetime_parser.cpp" 
tail functionality/cpp_datetime_parser/src/cpp_datetime_parser.cpp 

echo "\n# Attempt to build the component and report the results."
echo "[i] BASH SCRIPT:\n\tcd functionality/cpp_datetime_parser/build\n\trm -rf *\n\tcmake ..\n\tls -lart\n\tcmake --build .\n\tcd ../../../"
rm -rf functionality/cpp_datetime_parser/build/*
cd functionality/cpp_datetime_parser/build/
cmake ..

echo "\n# Display the result of cmake in the build/ directory, just before cmake --build ."
echo "[i] BASH COMMAND: ls -lart"
ls -lart
cmake --build .
cd ../../../

echo "\n# Showing CPP Index Call Directory Layout"
echo "[i] BASH COMMAND: ls -lart functionality/cpp_index_call/"
ls -lart functionality/cpp_index_call/
echo "[i] BASH COMMAND: ls -lart functionality/cpp_index_call/src/"
ls -lart functionality/cpp_index_call/src/

echo "\n# Showing CPP Index Call CMakeLists.txt Layout"
echo "[i] BASH COMMAND: cat functionality/cpp_index_call/CMakeLists.txt"
cat functionality/cpp_index_call/CMakeLists.txt

echo "\n# Prove that the tail of the cpp_index_call.cpp calls correct cpp module name"
echo "[i] BASH COMMAND: tail functionality/cpp_index_call/src/cpp_index_call.cpp"
tail functionality/cpp_index_call/src/cpp_index_call.cpp 

echo "\n# Attempt to build the cpp_index_call component and report the results."
echo "[i] BASH SCRIPT:\n\tcd functionality/cpp_index_call/build\n\trm -rf *\n\tcmake ..\n\tls -lart\n\tcmake --build .\n\tcd ../../../"
rm -rf functionality/cpp_index_call/build/*
cd functionality/cpp_index_call/build/
cmake ..

echo "\n# Display the result of cmake in the build/ directory, just before cmake --build ."
echo "[i] BASH COMMAND: ls -lart"
ls -lart
cmake --build .
cd ../../../

echo "\n# Showing the Layout of the indexes/* directory"
echo "[i] BASH COMMAND: find indexes/ -type d -exec sh -c 'echo \"\n=== {} ===\"; ls -lart {}' \;"
find indexes/ -type d -exec sh -c 'echo "\n=== {} ==="; ls -lart {}' \;

echo "\n# Lastly, print out the content of _test.py to show content and then attempt to run it with the just-built cpp components."
cd /home/cspeakes/test_project_root
echo "[i] BASH SCRIPT:\n\tcd ~/test_project_root\n\tcat _test.py"
cat _test0.py
echo "[i] BASH COMMAND: python3 _test.py"
python3 _test0.py

./describe_indexes.py
