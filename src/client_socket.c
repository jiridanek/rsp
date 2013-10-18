#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/epoll.h>
#include <netdb.h>
#include <string.h>


#include "netutils.h"
#include "epollinterface.h"
#include "backend_socket.h"
#include "client_socket.h"


#define BUFFER_SIZE 4096

struct data_buffer_entry {
    int is_close_message;
    char* data;
    int current_offset;
    int len;
    struct data_buffer_entry* next;
};


void really_close_client_socket(struct epoll_event_handler* self)
{
    struct client_socket_event_data* closure = (struct client_socket_event_data* ) self->closure;
    struct data_buffer_entry* next;
    while (closure->write_buffer != NULL) {
        next = closure->write_buffer->next;
        if (!closure->write_buffer->is_close_message) {
            free(closure->write_buffer->data);
        }
        free(closure->write_buffer);
        closure->write_buffer = next;
    }

    close(self->fd);
    free(self->closure);
    free(self);
}


void handle_client_socket_event(struct epoll_event_handler* self, uint32_t events)
{
    struct client_socket_event_data* closure = (struct client_socket_event_data* ) self->closure;
    if ((events & EPOLLOUT) && (closure->write_buffer != NULL)) {
        int written;
        int to_write;
        struct data_buffer_entry* temp;
        while (closure->write_buffer != NULL) {
            if (closure->write_buffer->is_close_message) {
                really_close_client_socket(self);
                return;
            }

            to_write = closure->write_buffer->len - closure->write_buffer->current_offset;
            written = write(self->fd, closure->write_buffer->data + closure->write_buffer->current_offset, to_write);
            if (written != to_write) {
                if (written == -1) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("Error writing to client");
                        exit(-1);
                    }
                    written = 0;
                }
                closure->write_buffer->current_offset += written;
                break;
            } else {
                temp = closure->write_buffer;
                closure->write_buffer = closure->write_buffer->next;
                free(temp->data);
                free(temp);
            }
        }
    }

    char read_buffer[BUFFER_SIZE];
    int bytes_read;

    if (events & EPOLLIN) {
        while ((bytes_read = read(self->fd, read_buffer, BUFFER_SIZE)) != -1 && bytes_read != 0) {
            if (bytes_read == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return;
            }

            if (bytes_read == 0 || bytes_read == -1) {
                close_backend_socket(closure->backend_handler);
                close_client_socket(self);
                return;
            }

            write(closure->backend_handler->fd, read_buffer, bytes_read);
        }
    }

    if ((events & EPOLLERR) | (events & EPOLLHUP) | (events & EPOLLRDHUP)) {
        close_backend_socket(closure->backend_handler);
        close_client_socket(self);
        return;
    }

}


void add_write_buffer_entry(struct client_socket_event_data* closure, struct data_buffer_entry* new_entry) 
{
    struct data_buffer_entry* last_buffer_entry;
    if (closure->write_buffer == NULL) {
        closure->write_buffer = new_entry;
    } else {
        for (last_buffer_entry=closure->write_buffer; last_buffer_entry->next != NULL; last_buffer_entry=last_buffer_entry->next)
            ;
        last_buffer_entry->next = new_entry;
    }
}


void write_to_client(struct epoll_event_handler* self, char* data, int len)
{
    struct client_socket_event_data* closure = (struct client_socket_event_data* ) self->closure;

    int written = 0;
    if (closure->write_buffer == NULL) {
        written = write(self->fd, data, len);
        if (written == len) {
            return;
        }
    }
    if (written == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("Error writing to client");
            exit(-1);
        }
        written = 0;
    }

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


void close_client_socket(struct epoll_event_handler* self)
{
    struct client_socket_event_data* closure = (struct client_socket_event_data* ) self->closure;
    if (closure->write_buffer == NULL) {
        really_close_client_socket(self);
    } else {
        struct data_buffer_entry* new_entry = malloc(sizeof(struct data_buffer_entry));
        new_entry->is_close_message = 1;
        new_entry->next = NULL;

        add_write_buffer_entry(closure, new_entry);
    }
}


struct epoll_event_handler* create_client_socket_handler(int epoll_fd, int client_socket_fd)
{
    
    make_socket_non_blocking(client_socket_fd);

    struct client_socket_event_data* closure = malloc(sizeof(struct client_socket_event_data));

    struct epoll_event_handler* result = malloc(sizeof(struct epoll_event_handler));
    result->fd = client_socket_fd;
    result->handle = handle_client_socket_event;
    result->closure = closure;

    closure->write_buffer = NULL;

    add_epoll_handler(epoll_fd, result, EPOLLIN | EPOLLRDHUP | EPOLLET | EPOLLOUT);

    return result;
}
