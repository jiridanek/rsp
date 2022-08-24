// https://github.com/apache/qpid-proton/blob/main/c/examples/raw_echo.c

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "thread.h"

#include <proton/condition.h>
#include <proton/raw_connection.h>
#include <proton/listener.h>
#include <proton/netaddr.h>
#include <proton/proactor.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

const size_t thread_count = 1;
enum {
    MAX_CONNECTIONS = 50,
    LISTENER_COUNT = 4,  // don't touch this!
    READ_BUFFERS = 8,  // bigger is better
    READ_BUFFER_SIZE = 32 * 1024,  // bigger is better
};

typedef struct app_data_t {
    pn_proactor_t *proactor;
    pn_listener_t *listeners[LISTENER_COUNT];
    const char *target_addrs[LISTENER_COUNT];

    pthread_mutex_t lock;
    int64_t first_idle_time;
    int64_t wake_conn_time;
    int connects;
    int disconnects;

    /* Sender values */

    /* Receiver values */
} app_data_t;

// half-proxy, responsible for moving data from incoming_connection to outcoming_connection
//
// there are two half-proxies for every connection pair, one for each direction
typedef struct conn_data_t {
    bool is_incoming;
    pn_raw_connection_t *incoming_connection;
    pn_raw_connection_t *outcoming_connection;
    struct conn_data_t *outcoming_connection_context;
    int64_t last_recv_time;

    // protects the variables below: flag, buffer lists and their counters
    pthread_mutex_t mutex;
    // when the connection is not open, we may not call _wake on it any more
    bool incoming_connection_open;
    // buffers with data read on this connection, waiting to be written to outcoming_connection
    pn_raw_buffer_t out_buffs[READ_BUFFERS];
    int out_buff_pointer;
    // buffers with data written by this connection, waiting to be handed to outcoming_connection for further reading
    pn_raw_buffer_t in_buffs[READ_BUFFERS];
    int in_buff_pointer;
    // end of mutex-protected things

    int bytes;
    int buffers;
} conn_data_t;

static conn_data_t conn_data[MAX_CONNECTIONS] = {{0}};

static int exit_code = 0;

/* Close the connection and the listener so so we will get a
 * PN_PROACTOR_INACTIVE event and exit, once all outstanding events
 * are processed.
 */
static void close_all(pn_raw_connection_t *c, app_data_t *app) {
    if (c) pn_raw_connection_close(c);
    for (int i=0; i < LISTENER_COUNT; i++) {
        if (app->listeners[i]) pn_listener_close(app->listeners[i]);
    }
}

static bool check_condition(pn_event_t *e, pn_condition_t *cond, app_data_t *app) {
    if (pn_condition_is_set(cond)) {
        fprintf(stderr, "%s: %s: %s\n", pn_event_type_name(pn_event_type(e)),
                pn_condition_get_name(cond), pn_condition_get_description(cond));
        return true;
    }

    return false;
}

static void check_condition_fatal(pn_event_t *e, pn_condition_t *cond, app_data_t *app) {
    if (check_condition(e, cond, app)) {
        close_all(pn_event_raw_connection(e), app);
        exit_code = 1;
    }
}

static void send_message(pn_raw_connection_t *c, const char* msg) {
    pn_raw_buffer_t buffer;
    uint32_t len = strlen(msg);
    char *buf = (char *) malloc(READ_BUFFER_SIZE);
    memcpy(buf, msg, len);
    buffer.bytes = buf;
    buffer.capacity = READ_BUFFER_SIZE;
    buffer.offset = 0;
    buffer.size = len;
    // If message not accepted just throw it away!
    if (pn_raw_connection_write_buffers(c, &buffer, 1) < 1) {
        printf("**Couldn't send message: write not accepted**\n");
        free(buf);
    }
}

static void recv_message(pn_raw_buffer_t buf) {
//    fwrite(buf.bytes, buf.size, 1, stdout);
}

conn_data_t *make_conn_data(pn_raw_connection_t *c) {
    int i;
    for (i = 0; i < MAX_CONNECTIONS; ++i) {
        if (!conn_data[i].incoming_connection) {
            conn_data[i].incoming_connection = c;
            pthread_mutex_init(&(conn_data[i].mutex), NULL);
            conn_data[i].incoming_connection_open = true;
            return &conn_data[i];
        }
    }
    return NULL;
}

void free_conn_data(conn_data_t *c) {
    if (!c) return;
    pthread_mutex_destroy(&(c->mutex));
    c->incoming_connection = NULL;
    c->outcoming_connection = NULL;
}

static void free_buffers(pn_raw_buffer_t buffs[], size_t n) {
    unsigned i;
    for (i=0; i<n; ++i) {
        free(buffs[i].bytes);
    }
}

/* This function handles events when we are acting as the receiver */
static void handle_receive(app_data_t *app, pn_event_t* event) {
    switch (pn_event_type(event)) {

        case PN_RAW_CONNECTION_CONNECTED: {
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            conn_data_t *cd = (conn_data_t *) pn_raw_connection_get_context(c);
            pn_raw_buffer_t buffers[READ_BUFFERS] = {{0}};
            if (cd) {
                if (cd->is_incoming) {
                    printf("**incoming raw connection %tu connected\n", cd - conn_data);
                } else {
                    printf("**outcoming raw connection %tu connected\n", cd - conn_data);
                }
                int i = READ_BUFFERS;
                pthread_mutex_lock(&app->lock);
                app->connects++;
                pthread_mutex_unlock(&app->lock);
                for (; i; --i) {
                    pn_raw_buffer_t *buff = &buffers[READ_BUFFERS - i];
                    buff->bytes = (char *) malloc(READ_BUFFER_SIZE);
                    buff->capacity = READ_BUFFER_SIZE;
                    buff->size = 0;
                    buff->offset = 0;
                }
                pn_raw_connection_give_read_buffers(c, buffers, READ_BUFFERS);
            } else {
                printf("**too many raw connections connected: closing\n");
                pn_raw_connection_close(c);
            }
        }
            break;

        case PN_RAW_CONNECTION_WAKE: {
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            conn_data_t *cd = (conn_data_t *) pn_raw_connection_get_context(c);
            conn_data_t *ocd = cd->outcoming_connection_context;

            bool is_write_closed = pn_raw_connection_is_write_closed(c);
            bool is_read_closed = pn_raw_connection_is_read_closed(c);

            pthread_mutex_lock(&ocd->mutex);
            if (ocd->out_buff_pointer > 0 && !is_write_closed) {
                pn_raw_connection_write_buffers(c, ocd->out_buffs, ocd->out_buff_pointer);
                ocd->out_buff_pointer = 0;
            } else {
                free_buffers(ocd->out_buffs, ocd->out_buff_pointer);
                ocd->out_buff_pointer = 0;
            }

//            pthread_mutex_unlock(&ocd->mutex);
//            pthread_mutex_lock(&ocd->mutex);

            if (ocd->in_buff_pointer > 0 && !is_read_closed) {
                pn_raw_connection_give_read_buffers(c, ocd->in_buffs, ocd->in_buff_pointer);
                ocd->in_buff_pointer = 0;
            } else {
                free_buffers(ocd->in_buffs, ocd->in_buff_pointer);
                ocd->in_buff_pointer = 0;
            }
            pthread_mutex_unlock(&ocd->mutex);

//        don't log too much
//            printf("**raw connection %tu woken\n", cd-conn_data);
        } break;

        case PN_RAW_CONNECTION_DISCONNECTED: {
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            conn_data_t *cd = (conn_data_t *) pn_raw_connection_get_context(c);
            if (cd) {
                pthread_mutex_lock(&app->lock);
                app->disconnects++;
                pthread_mutex_unlock(&app->lock);
//                pn_raw_connection_write_close(cd->outcoming_connection);
                printf("**raw connection %tu disconnected: bytes: %d, buffers: %d\n", cd - conn_data, cd->bytes,
                       cd->buffers);
            } else {
                printf("**raw connection disconnected: not connected\n");
            }
            check_condition(event, pn_raw_connection_condition(c), app);
            pn_raw_connection_wake(c);
//            pn_raw_connection_wake(cd->outcoming_connection);
            free_conn_data(cd);
        } break;

        case PN_RAW_CONNECTION_DRAIN_BUFFERS: {
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            pn_raw_buffer_t buffs[READ_BUFFERS];
            size_t n;
            while ( (n = pn_raw_connection_take_read_buffers(c, buffs, READ_BUFFERS)) ) {
                free_buffers(buffs, n);
            }
            while ( (n = pn_raw_connection_take_written_buffers(c, buffs, READ_BUFFERS)) ) {
                free_buffers(buffs, n);
            }
        }

        case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        } break;

        case PN_RAW_CONNECTION_READ: {
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            conn_data_t *cd = (conn_data_t *) pn_raw_connection_get_context(c);
            pn_raw_buffer_t buffs[READ_BUFFERS];
            size_t n;
            bool need_wake = false;
            cd->last_recv_time = pn_proactor_now_64();
            while ( (n = pn_raw_connection_take_read_buffers(c, buffs, READ_BUFFERS)) ) {
                unsigned i;
                pthread_mutex_lock(&cd->mutex);
                for (i = 0; i < n && buffs[i].bytes; ++i) {
                    cd->bytes += buffs[i].size;
                    // store the buffer to be written later
                    cd->out_buffs[cd->out_buff_pointer++] = buffs[i];
                }
                pthread_mutex_unlock(&cd->mutex);
                cd->buffers += n;

                need_wake = true;
            }

            if (need_wake) {
                // tell the other connection there is something for it to do
                bool outcoming_connection_open = false;
                pthread_mutex_lock(&cd->outcoming_connection_context->mutex);
                outcoming_connection_open = cd->outcoming_connection_context->incoming_connection_open;
                pthread_mutex_unlock(&cd->outcoming_connection_context->mutex);
                if (outcoming_connection_open) {
                    pn_raw_connection_wake(cd->outcoming_connection);
                }
            }
        } break;

        case PN_RAW_CONNECTION_CLOSED_READ: {
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            if (!pn_raw_connection_is_write_closed(c)) {
//                send_message(c, "** Goodbye **");
            }
        }
        case PN_RAW_CONNECTION_CLOSED_WRITE:{
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            conn_data_t *cd = (conn_data_t *) pn_raw_connection_get_context(c);
            pthread_mutex_lock(&(cd->mutex));
            cd->incoming_connection_open = false;
            pthread_mutex_unlock(&(cd->mutex));
            pn_raw_connection_close(c);
        } break;
        case PN_RAW_CONNECTION_WRITTEN: {
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            conn_data_t *cd = (conn_data_t *) pn_raw_connection_get_context(c);
            pn_raw_buffer_t buffs[READ_BUFFERS];
            bool need_wake = false;
            size_t n;
            while ((n = pn_raw_connection_take_written_buffers(c, buffs, READ_BUFFERS))) {
                pthread_mutex_lock(&cd->mutex);
                for (int i = 0; i < n; i++) {
                    cd->in_buffs[cd->in_buff_pointer++] = buffs[i];
                }
                pthread_mutex_unlock(&cd->mutex);
                need_wake = true;
            }
            // if we wrote anything, inform the other connection
            if (need_wake) {
                bool outcoming_connection_open = false;
                pthread_mutex_lock(&cd->outcoming_connection_context->mutex);
                outcoming_connection_open = cd->outcoming_connection_context->incoming_connection_open;
                pthread_mutex_unlock(&cd->outcoming_connection_context->mutex);
                if (outcoming_connection_open) {
                    pn_raw_connection_wake(cd->outcoming_connection);
                }
            }
        } break;
        default:
            break;
    }
}

/* Handle all events, delegate to handle_send or handle_receive
   Return true to continue, false to exit
*/
static bool handle(app_data_t* app, pn_event_t* event) {
    switch (pn_event_type(event)) {

        case PN_LISTENER_OPEN: {
            char port[256];             /* Get the listening port */
            pn_netaddr_host_port(pn_listener_addr(pn_event_listener(event)), NULL, 0, port, sizeof(port));
            printf("**listening on %s\n", port);
            fflush(stdout);
            break;
        }
        case PN_LISTENER_ACCEPT: {
            pn_proactor_t *proactor = pn_event_proactor(event);
            pn_listener_t *listener = pn_event_listener(event);

            // make_conn_data touches the global array of connections
            pthread_mutex_lock(&app->lock);

            pn_raw_connection_t *c = pn_raw_connection();
            conn_data_t *icd = make_conn_data(c);

            pn_raw_connection_t *o = pn_raw_connection();
            conn_data_t *ocd = make_conn_data(o);

            if (icd == NULL || ocd == NULL) {
                printf("**too many connections, trying again later...\n");

                /* No other sensible/correct way to reject connection - have to defer closing to event handler */
                pn_raw_connection_set_context(c, 0);
                pn_listener_raw_accept(listener, c);
            }

            const int i = (int) pn_listener_get_context(listener);
            const char *target_addr = app->target_addrs[i];

            icd->is_incoming = true;
            icd->outcoming_connection = o;
            icd->outcoming_connection_context = ocd;
            ocd->is_incoming = false;
            ocd->outcoming_connection = c;
            ocd->outcoming_connection_context = icd;

            int64_t now = pn_proactor_now_64();

            app->first_idle_time = 0;
            if (app->wake_conn_time < now) {
                app->wake_conn_time = now + 5000;
                pn_proactor_set_timeout(pn_listener_proactor(listener), 5000);
            }
            pn_raw_connection_set_context(c, icd);
            pn_raw_connection_set_context(o, ocd);

            pn_listener_raw_accept(listener, c);
            pthread_mutex_unlock(&app->lock);

            // first set up everything, only then initiate outcoming connection
            pn_proactor_raw_connect(proactor, o, target_addr);

        } break;

        case PN_LISTENER_CLOSE: {
            pn_listener_t *listener = pn_event_listener(event);
            for (int i=0; i < LISTENER_COUNT; i++) {
                if (listener == app->listeners[i]) {
                    app->listeners[i] = NULL;        /* Listener is closed */
                }
            }
            printf("**listener closed\n");
            check_condition_fatal(event, pn_listener_condition(listener), app);
        } break;

//        case PN_PROACTOR_TIMEOUT: {
//            pn_proactor_t *proactor = pn_event_proactor(event);
//            pthread_mutex_lock(&app->lock);
//            int64_t now = pn_proactor_now_64();
//            pn_millis_t timeout = 5000;
//            if (app->connects - app->disconnects == 0) {
//                timeout = 20000;
//                if (app->first_idle_time == 0) {
//                    printf("**idle detected, shutting down in %dms\n", timeout);
//                    app->first_idle_time = now;
//                } else if (app->first_idle_time + 20000 <= now) {
//                    printf("**no activity for %dms: shutting down now\n", timeout);
//                    for (int i = 0; i< LISTENER_COUNT; i++) {
//                        pn_listener_close(app->listeners[i]);
//                    }
//                    break; // No more timeouts
//                }
//            } else if (now >= app->wake_conn_time) {
//                int i;
//                for (i = 0; i < MAX_CONNECTIONS; ++i) {
//                    if (conn_data[i].incoming_connection) {
//                        pn_raw_connection_wake(conn_data[i].incoming_connection);
//                    }
//                }
//                app->wake_conn_time = now + 5000;
//            }
//            pn_proactor_set_timeout(proactor, timeout);
//            pthread_mutex_unlock(&app->lock);
//        }  break;

        case PN_PROACTOR_INACTIVE:
        case PN_PROACTOR_INTERRUPT: {
            pn_proactor_t *proactor = pn_event_proactor(event);
            pn_proactor_interrupt(proactor);
            return false;
        } break;

        default: {
            pn_raw_connection_t *c = pn_event_raw_connection(event);
            if (c) {
                handle_receive(app, event);
            }
        }
    }
    return exit_code == 0;
}

void* run(void *arg) {
    app_data_t *app = arg;

    /* Loop and handle events */
    bool again = true;
    do {
        pn_event_batch_t *events = pn_proactor_wait(app->proactor);
        pn_event_t *e;
        for (e = pn_event_batch_next(events); e && again; e = pn_event_batch_next(events)) {
            again = handle(app, e);
        }
        pn_proactor_done(app->proactor, events);
    } while(again);
    return NULL;
}

static void add_listener(app_data_t *app, int i, const char *addr, const char *target_addr) {
    assert (i < LISTENER_COUNT);
    app->target_addrs[i] = target_addr;
    pn_proactor_listen(app->proactor, app->listeners[i], addr, 16);
    pn_listener_set_context(app->listeners[i], (void *) i);
}

int main(int argc, char **argv) {

    struct app_data_t app = {0};
    pthread_mutex_init(&app.lock, NULL);

    /* Create the proactor and connect */
    app.proactor = pn_proactor();
    for (int i = 0; i < LISTENER_COUNT; i++) {
        app.listeners[i] = pn_listener();
    }
    add_listener(&app, 0, "localhost:5101", "127.0.0.1:5201");
    add_listener(&app, 1, "localhost:5102", "127.0.0.1:5202");
    add_listener(&app, 2, "localhost:5103", "127.0.0.1:5203");
    add_listener(&app, 3, "localhost:5104", "127.0.0.1:5204");

    pthread_t *threads = (pthread_t *) calloc(sizeof(pthread_t), thread_count);
    int n;
    for (n = 0; n < thread_count; n++) {
        int rc = pthread_create(&threads[n], 0, run, (void *) &app);
        if (rc) {
            fprintf(stderr, "Failed to create thread\n");
            exit(-1);
        }
    }

//    don't do work on main thread
//    run(&app);

    for (n = 0; n < thread_count; n++) {
        pthread_join(threads[n], 0);
    }
    free(threads);

    pn_proactor_free(app.proactor);
    return exit_code;
}
