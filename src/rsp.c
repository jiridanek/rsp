#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <signal.h>

#include "epollinterface.h"
#include "logging.h"
#include "server_socket.h"


int main()
{

    char *argv[] = {
            "5101",
            "localhost",
            "5201",
            NULL,
    };

    char *server_port_str = argv[1];
    char *backend_addr = argv[2];
    char *backend_port_str = argv[3];

    // https://stackoverflow.com/questions/21687695/getting-sigpipe-with-non-blocking-sockets-is-this-normal/
    signal(SIGPIPE, SIG_IGN);

    epoll_init();

    create_server_socket_handler(server_port_str,
                                 backend_addr,
                                 backend_port_str);

    rsp_log("Started.  Listening on port %s.", server_port_str);
    epoll_do_reactor_loop();

    return 0;
}

