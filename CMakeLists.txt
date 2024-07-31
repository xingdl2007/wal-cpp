# Copyright 2017 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.9)
# Keep the version below in sync with the one in db.h
project(wal-cpp VERSION 0.0.1 LANGUAGES C CXX)

# C++ standard can be overridden when this is used as a sub-project.
if (NOT CMAKE_CXX_STANDARD)
    # This project requires C++11.
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif (NOT CMAKE_CXX_STANDARD)

add_library(wal "")
target_sources(wal PRIVATE
        "src/wal.cpp"
        "src/wal.h"
        "src/slice.h"
        "src/crc32.cpp"
        "src/crc32.h"
        "src/status.cpp"
        "src/status.h"
)

add_executable(wal-cpp "main.cpp" "util.h")
target_link_libraries(wal-cpp wal pthread)