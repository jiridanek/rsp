set(Proton_DIR "/home/jdanek/repos/qpid/qpid-proton/build/install/lib64/cmake/Proton")

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)

find_package(Threads REQUIRED)
find_package(Proton 0.37.0 REQUIRED COMPONENTS Core Proactor)

add_executable(proton
        main.c)

target_link_libraries(proton Threads::Threads Proton::core Proton::proactor)
