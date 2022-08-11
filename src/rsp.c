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
            "cheating-router",
            "5101",
            "127.0.0.1",
            "5201",
            NULL,
    };

    char *server_port_str = argv[1];
    char *backend_addr = argv[2];
    char *backend_port_str = argv[3];

    // https://idea.popcount.org/2017-02-20-epoll-is-fundamentally-broken-12/
    /*
     * > Additionally, in edge triggered mode it's hard to avoid starvation:
     */

    // https://stackoverflow.com/questions/21687695/getting-sigpipe-with-non-blocking-sockets-is-this-normal/
    signal(SIGPIPE, SIG_IGN);

    epoll_init();

    create_server_socket_handler("5101", "127.0.0.1", "5201");
    create_server_socket_handler("5102", "127.0.0.1", "5202");
    create_server_socket_handler("5103", "127.0.0.1", "5203");
    create_server_socket_handler("5104", "127.0.0.1", "5204");

    rsp_log("Started.  Listening on port %s and maybe few other ones.", server_port_str);
    epoll_do_reactor_loop();

    return 0;
}

