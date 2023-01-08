cmake_minimum_required(VERSION 3.14)
project(tongDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS -pthread)

include_directories(
        antlr4
)
set(src_dir
        parser/SQLBaseVisitor.cpp
        parser/SQLLexer.cpp
        parser/SQLParser.cpp
        filesystem/FileSystem.cpp
        recordsystem/RecordHandle.cpp
        recordsystem/RecordManager.cpp
        indexsystem/IndexHandle.cpp
        indexsystem/IndexManager.cpp
        managesystem/ManageSystem.cpp
        querysystem/QuerySystem.cpp
        main.cpp
        )
file(GLOB antlr4-cpp-src
        antlr4/*.cpp
        antlr4/atn/*.cpp
        antlr4/dfa/*.cpp
        antlr4/internal/*.cpp
        antlr4/misc/*.cpp
        antlr4/support/*.cpp
        antlr4/tree/*.cpp
        antlr4/tree/pattern/*.cpp
        antlr4/tree/xpath/*.cpp
        )
add_library (antlr4-cpp-runtime ${antlr4-cpp-src})
add_executable(tongDB ${src_dir})
target_link_libraries(tongDB antlr4-cpp-runtime)
