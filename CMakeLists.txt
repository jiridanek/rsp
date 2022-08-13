cmake_minimum_required(VERSION 3.16)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_EXTENSIONS OFF)

project(
  cheating-router
  VERSION 0.0.1
  DESCRIPTION ""
  LANGUAGES C)

add_subdirectory(proton)

add_executable(cheating-router
        src/connection.c
        src/epollinterface.c
        src/logging.c
        src/netutils.c
        src/rsp.c
        src/server_socket.c)