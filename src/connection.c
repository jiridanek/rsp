#define _GNU_SOURCE         /* See feature_test_macros(7) */

#include <fcntl.h>

#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/epoll.h>
#include <netdb.h>
#include <string.h>
#include <stdbool.h>


#include "epollinterface.h"
#include "connection.h"
#include "logging.h"
#include "netutils.h"


const int BUFFER_SIZE = 32 * 1024;

struct data_buffer_entry {
    int is_close_message;
    char* data;
    int current_offset;
    int len;
    struct data_buffer_entry* next;
};


void connection_really_close(struct epoll_event_handler* self)
{
    struct connection_closure *closure = (struct connection_closure *) self->closure;
    struct data_buffer_entry *next;
    while (closure->write_buffer != NULL) {
        next = closure->write_buffer->next;
        if (!closure->write_buffer->is_close_message) {
            epoll_add_to_free_list(closure->write_buffer->data);
        }
        epoll_add_to_free_list(closure->write_buffer);
        closure->write_buffer = next;
    }
    close(closure->pipefd[0]);
    close(closure->pipefd[1]);

    epoll_remove_handler(self);
    close(self->fd);
    epoll_add_to_free_list(self->closure);
    epoll_add_to_free_list(self);
    rsp_log("Freed connection %p", self);
}


void connection_on_close_event(struct epoll_event_handler* self)
{
    struct connection_closure* closure = (struct connection_closure*) self->closure;
    if (closure->on_close != NULL) {
        closure->on_close(closure->on_close_closure);
    }
    connection_close(self);
}

// original version of in event handling has unlimited buffer space...
void connection_on_out_event(struct epoll_event_handler* self)
{
    perror("can't do this, dave");
//    exit(1);

    struct connection_closure* closure = (struct connection_closure*) self->closure;
    int written;
    int to_write;
    struct data_buffer_entry* temp;
    while (closure->write_buffer != NULL) {
        if (closure->write_buffer->is_close_message) {
            connection_really_close(self);
            return;
        }

        to_write = closure->write_buffer->len - closure->write_buffer->current_offset;
        written = write(self->fd, closure->write_buffer->data + closure->write_buffer->current_offset, to_write);
        if (written != to_write) {
            if (written == -1) {
                if (errno == ECONNRESET || errno == EPIPE) {
                    rsp_log_error("On out event write error");
                    connection_on_close_event(self);
                    return;
                }
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    rsp_log_error("Error writing to client");
                    exit(-1);
                }
                written = 0;
            }
            closure->write_buffer->current_offset += written;
            break;
        } else {
            temp = closure->write_buffer;
            closure->write_buffer = closure->write_buffer->next;
            epoll_add_to_free_list(temp->data);
            epoll_add_to_free_list(temp);
        }
    }
}


void connection_on_in_event(struct epoll_event_handler* self) {
    struct connection_closure *closure = (struct connection_closure *) self->closure;

    // https://blog.superpat.com/zero-copy-in-linux-with-sendfile-and-splice
    while(true) {
        ssize_t bytes_read;
        if ((bytes_read = splice(self->fd, NULL, closure->pipefd[1], NULL,
                                 8 * 1024,
                                 SPLICE_F_MORE | SPLICE_F_MOVE)) <= 0) {
            if (errno == EINTR || errno == EAGAIN) {
                // Interrupted system call/try again
                // we would get epoll event to continue later, so lets quit for now
                return;
            }

            perror("splice");
            exit(1);

        }

        if (bytes_read == 0 || bytes_read == -1) {
            connection_on_close_event(self);
            return;
        }

        // extract read_closure
        struct proxy_data *read_closure = (struct proxy_data *) closure->on_read_closure;

        ssize_t bytes_in_pipe = bytes_read;
        while (bytes_in_pipe > 0) {
            ssize_t bytes;
            int fdout = self->fd == read_closure->client->fd ? read_closure->backend->fd : read_closure->client->fd;
            if ((bytes = splice(closure->pipefd[0], NULL, fdout, NULL, bytes_in_pipe,
                                SPLICE_F_MORE | SPLICE_F_MOVE)) <= 0) {
                if (errno == EINTR || errno == EAGAIN) {
                    // Interrupted system call/try again
                    // we will get epoll event to empty this later
//                    return;
                    continue; // lets actually retry this
                }
                perror("splice");
                exit(-1);
            }
            bytes_in_pipe -= bytes;
        }
    }
}


//    total_bytes_sent += bytes_sent;

//    char read_buffer[BUFFER_SIZE];
//    int bytes_read;
//
//    while ((bytes_read = read(self->fd, read_buffer, BUFFER_SIZE)) != -1 && bytes_read != 0) {
//        if (bytes_read == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
//            return;
//        }
//
//        if (bytes_read == 0 || bytes_read == -1) {
//            connection_on_close_event(self);
//            return;
//        }
//
//        if (bytes_read < BUFFER_SIZE) {
//            printf("read %d, remaining %d\n", bytes_read, BUFFER_SIZE - bytes_read);
//        }
//
//        if (closure->on_read != NULL) {
//            closure->on_read(closure->on_read_closure, read_buffer, bytes_read);
//        }
//    }
//}


void connection_handle_event(struct epoll_event_handler *self, uint32_t events) {
    if (events & EPOLLOUT) {
        connection_on_out_event(self);
    }

    if (events & EPOLLIN) {
        connection_on_in_event(self);
    }

    if ((events & EPOLLERR) | (events & EPOLLHUP) | (events & EPOLLRDHUP)) {
        connection_on_close_event(self);
    }

}


void add_write_buffer_entry(struct connection_closure* closure, struct data_buffer_entry* new_entry) {
    if (closure->write_buffer == NULL) {
        closure->write_buffer = new_entry;
        closure->last_buffer_entry = new_entry;
    } else {
        closure->last_buffer_entry->next = new_entry;
        closure->last_buffer_entry = new_entry;
    }

    int c = 0;
    for (struct data_buffer_entry *e = closure->write_buffer; e != NULL; e = e->next) {
        c++;
    }
//    printf("have %d buffers\n", c);
}


void connection_write(struct epoll_event_handler* self, char* data, int len)
{
    struct connection_closure* closure = (struct connection_closure* ) self->closure;

    int written = 0;
    if (closure->write_buffer == NULL) {
        written = write(self->fd, data, len);
        if (written == len) {
            return;
        }
    }
    if (written == -1) {
        if (errno == ECONNRESET || errno == EPIPE) {
            rsp_log_error("Connection write error");
            connection_on_close_event(self);
            return;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            rsp_log_error("Error writing to client");
            exit(-1);
        }
        written = 0;
    }

    // so, it tries to write as much as it can, and then buffers what does not fit into outgoing socket, meaning we get epoll event later to write this
    int unwritten = len - written;
    struct data_buffer_entry* new_entry = malloc(sizeof(struct data_buffer_entry));
    new_entry->is_close_message = 0;
    new_entry->data = malloc(unwritten);
    memcpy(new_entry->data, data + written, unwritten);
    new_entry->current_offset = 0;
    new_entry->len = unwritten;
    new_entry->next = NULL;

    add_write_buffer_entry(closure, new_entry);
}


void connection_close(struct epoll_event_handler* self)
{
    struct connection_closure* closure = (struct connection_closure* ) self->closure;
    closure->on_read = NULL;
    closure->on_close = NULL;
    if (closure->write_buffer == NULL) {
        connection_really_close(self);
    } else {
        struct data_buffer_entry* new_entry = malloc(sizeof(struct data_buffer_entry));
        new_entry->is_close_message = 1;
        new_entry->next = NULL;

        add_write_buffer_entry(closure, new_entry);
    }
}


struct epoll_event_handler* create_connection(int client_socket_fd) {
    make_socket_non_blocking(client_socket_fd);

    struct connection_closure *closure = malloc(sizeof(struct connection_closure));
    closure->write_buffer = NULL;
    // Setup the pipe at initialization time
    if (pipe(closure->pipefd) < 0) {
        perror("pipe");
        exit(1);
    }

    struct epoll_event_handler *result = malloc(sizeof(struct epoll_event_handler));
    rsp_log("Created connection epoll handler %p", result);
    result->fd = client_socket_fd;
    result->handle = connection_handle_event;
    result->closure = closure;


    epoll_add_handler(result, EPOLLIN | EPOLLRDHUP | EPOLLET | EPOLLOUT);

    return result;
}
