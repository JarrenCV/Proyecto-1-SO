#ifndef BROKER_H
#define BROKER_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>
#include <stdarg.h>

#define MAXIMO_MENSAJE 256
#define TAMANO_COLA 10
#define BROKER_PORT 5000
#define MAX_CONSUMERS 100

typedef struct {
    int id;
    char contenido[MAXIMO_MENSAJE];
} Mensajillo;

typedef struct {
    Mensajillo messages[TAMANO_COLA];
    int pleer;
    int plibre;
    pthread_mutex_t mutexCola;
} ColaMensajillos;

ColaMensajillos *cola = NULL;
int mensaje_id_global = 1; // ID consecutivo para los mensajes
pthread_mutex_t id_mutex = PTHREAD_MUTEX_INITIALIZER;

static int consumer_sockets[MAX_CONSUMERS];
static pthread_mutex_t consumers_mutex = PTHREAD_MUTEX_INITIALIZER;

int estaRellenita(ColaMensajillos *cola) {
    return (cola->plibre + 1) % TAMANO_COLA == cola->pleer;
}

int noTieneElementos(ColaMensajillos *cola) {
    return cola->pleer == cola->plibre;
}

void guardar_en_logillo(Mensajillo* mensaje) {
    FILE* log = fopen("mensajes.log", "a");
    if (log != NULL) {
        fprintf(log, "%s\n", mensaje->contenido);
        fclose(log);
    } else {
        printf("No se pudo abrir el archivillo\n");
    }
}

int insertar_mensajillo(ColaMensajillos *cola, Mensajillo *nuevo) {
    int ok = 0;
    pthread_mutex_lock(&cola->mutexCola);
    if (!estaRellenita(cola)) {
        cola->messages[cola->plibre] = *nuevo;
        cola->plibre = (cola->plibre + 1) % TAMANO_COLA;
        ok = 1;
    } else {
        printf("Colilla rellenita, no cabe más\n");
    }
    pthread_mutex_unlock(&cola->mutexCola);
    return ok;
}

int consumir_mensajillo(ColaMensajillos *cola, Mensajillo *destino) {
    int resultado = 0;
    pthread_mutex_lock(&cola->mutexCola);
    if (!noTieneElementos(cola)) {
        *destino = cola->messages[cola->pleer];
        cola->pleer = (cola->pleer + 1) % TAMANO_COLA;
        resultado = 1;
    }
    pthread_mutex_unlock(&cola->mutexCola);
    return resultado;
}

void inicializar_cola(ColaMensajillos *cola) {
    cola->pleer = 0;
    cola->plibre = 0;
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&cola->mutexCola, &attr);
    pthread_mutexattr_destroy(&attr);
}

// Añade un nuevo consumer a la lista
void agregar_consumer(int sockfd) {
    pthread_mutex_lock(&consumers_mutex);
    for (int i = 0; i < MAX_CONSUMERS; ++i) {
        if (consumer_sockets[i] < 0) {
            consumer_sockets[i] = sockfd;
            break;
        }
    }
    pthread_mutex_unlock(&consumers_mutex);
}

// Elimina un consumer de la lista
void quitar_consumer(int sockfd) {
    pthread_mutex_lock(&consumers_mutex);
    for (int i = 0; i < MAX_CONSUMERS; ++i) {
        if (consumer_sockets[i] == sockfd) {
            consumer_sockets[i] = -1;
            break;
        }
    }
    pthread_mutex_unlock(&consumers_mutex);
}

static void guardar_log(const char *fmt, ...);

// Envía el mensaje a todos los consumers conectados
void enviar_a_todos_consumers(Mensajillo *msg) {
    pthread_mutex_lock(&consumers_mutex);
    for (int i = 0; i < MAX_CONSUMERS; ++i) {
        int cs = consumer_sockets[i];
        if (cs >= 0) {
            printf("[Broker] Enviando a consumer fd=%d: id=%d, contenido=%s\n",
                   cs, msg->id, msg->contenido);
            if (send(cs, msg, sizeof(Mensajillo), 0) != sizeof(Mensajillo)) {
                close(cs);
                consumer_sockets[i] = -1;
            } else {
                guardar_log("ENVIADO consumer fd=%d id=%d contenido=\"%s\"",
                            cs, msg->id, msg->contenido);
            }
        }
    }
    pthread_mutex_unlock(&consumers_mutex);
}

static void guardar_log(const char *fmt, ...) {
    FILE *f = fopen("broker.log", "a");
    if (!f) {
        perror("abrir broker.log");
        return;
    }
    va_list ap;
    va_start(ap, fmt);
    vfprintf(f, fmt, ap);
    fprintf(f, "\n");
    va_end(ap);
    fclose(f);
}

// Hilo para manejar cada conexión
void *atender_cliente(void *arg) {
    int clientfd = *(int *)arg;
    free(arg);

    char buffer[sizeof(Mensajillo)];
    ssize_t n = recv(clientfd, buffer, sizeof(Mensajillo), MSG_PEEK);
    if (n <= 0) {
        close(clientfd);
        pthread_exit(NULL);
    }

    // rama producer
    if (n >= (ssize_t)sizeof(Mensajillo)) {
        Mensajillo recibido;
        recv(clientfd, &recibido, sizeof(Mensajillo), 0);

        // 1) asignar ID
        pthread_mutex_lock(&id_mutex);
        recibido.id = mensaje_id_global++;
        pthread_mutex_unlock(&id_mutex);

        // 2) LOG de recepción **antes** de reenviar
        {
            struct sockaddr_in sa; socklen_t salen = sizeof(sa);
            if (getpeername(clientfd, (struct sockaddr*)&sa, &salen) == 0) {
                char hip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &sa.sin_addr, hip, sizeof(hip));
                int hport = ntohs(sa.sin_port);
                guardar_log("RECIBIDO producer %s:%d id=%d contenido=\"%s\"",
                            hip, hport, recibido.id, recibido.contenido);
            }
        }

        // 3) insertar en cola, reenviar y liberar espacio
        if (insertar_mensajillo(cola, &recibido)) {
            enviar_a_todos_consumers(&recibido);

            // ahora eliminamos el mensaje de la cola para liberar espacio
            Mensajillo descartado;
            if (!consumir_mensajillo(cola, &descartado)) {
                fprintf(stderr, "Error: no se pudo liberar mensaje de la cola\n");
            }
        }

        close(clientfd);
        pthread_exit(NULL);
    }

    // si no era un Mensajillo, probablemente es un consumer pidiendo consumir
    n = recv(clientfd, buffer, sizeof(buffer), 0);
    if (n > 0 && strncmp(buffer, "CONSUMIR", 8) == 0) {
        agregar_consumer(clientfd);
        guardar_log("CONSUMIDOR fd=%d conectado", clientfd);

        // ahora esperamos ACKs por esa misma conexión
        while (1) {
            char ackbuf[64];
            ssize_t r = recv(clientfd, ackbuf, sizeof(ackbuf), 0);
            if (r <= 0) {
                // desconexión
                quitar_consumer(clientfd);
                guardar_log("CONSUMIDOR fd=%d desconectado", clientfd);
                break;
            }
            if (strncmp(ackbuf, "ACK ", 4) == 0) {
                int ack_id = atoi(ackbuf + 4);
                guardar_log("RECIBIDO_ACK consumer fd=%d id=%d", clientfd, ack_id);
            }
        }
    } else {
        close(clientfd);
    }
    pthread_exit(NULL);
}

int main() {
    // Memoria compartida local (no se usa entre procesos aquí, pero puedes adaptarlo)
    cola = mmap(NULL, sizeof(ColaMensajillos), PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (cola == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }
    inicializar_cola(cola);
    // Inicializar lista de consumers
    for (int i = 0; i < MAX_CONSUMERS; ++i) consumer_sockets[i] = -1;

    printf("Broker iniciado. Esperando conexiones en el puerto %d...\n", BROKER_PORT);

    int serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverfd < 0) {
        perror("socket");
        exit(1);
    }

    int opt = 1;
    setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(BROKER_PORT);

    if (bind(serverfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind");
        exit(1);
    }

    if (listen(serverfd, 10) < 0) {
        perror("listen");
        exit(1);
    }

    while (1) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int *clientfd = malloc(sizeof(int));
        *clientfd = accept(serverfd, (struct sockaddr *)&cli_addr, &cli_len);
        if (*clientfd < 0) {
            perror("accept");
            free(clientfd);
            continue;
        }
        pthread_t tid;
        pthread_create(&tid, NULL, atender_cliente, clientfd);
        pthread_detach(tid);
    }

    close(serverfd);
    munmap(cola, sizeof(ColaMensajillos));
    return 0;
}

#endif