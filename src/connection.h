struct connection_closure {
    void (*on_read)(void* closure, char* buffer, int len);
    void* on_read_closure;

    void (*on_close)(void* closure);
    void* on_close_closure;
    
    struct data_buffer_entry* write_buffer;
    struct data_buffer_entry* last_buffer_entry;

    int pipefd[2];
};

// need to leak private struct
struct proxy_data {
    struct epoll_event_handler* client;
    struct epoll_event_handler* backend;
};

extern void connection_write(struct epoll_event_handler* self, char* data, int len);

extern void connection_close(struct epoll_event_handler* self);

extern struct epoll_event_handler* create_connection(int connection_fd);
