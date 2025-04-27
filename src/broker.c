#ifndef BROKER_H
#define BROKER_H

#include <stdio.h>
#include <stdlib.h>    // ya estaba, para qsort()
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
#include <limits.h>   // para qsort

// Prototipo único de guardar_log(), debe estar antes de cualquier llamada
static void guardar_log(const char *fmt, ...);

#define MAXIMO_MENSAJE 256
#define TAMANO_COLA 10
#define BROKER_PORT 5000
#define MAX_GRUPOS 3
#define MAX_CONSUMERS_PER_GROUP 100

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

typedef struct {
    char nombre[32];
    int sockets[MAX_CONSUMERS_PER_GROUP];
    int count;
    int offset;
    pthread_mutex_t mutex;
} GrupoConsumers;

static GrupoConsumers grupos[MAX_GRUPOS];
static int num_grupos = 0;
static pthread_mutex_t grupos_mutex = PTHREAD_MUTEX_INITIALIZER;

int estaRellenita(ColaMensajillos *cola) {
    return (cola->plibre + 1) % TAMANO_COLA == cola->pleer;
}

int noTieneElementos(ColaMensajillos *cola) {
    return cola->pleer == cola->plibre;
}

static void guardar_en_logillo(Mensajillo* mensaje) {
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
        // Aviso en terminal
        printf("COLA_LLENA id_mensaje=%d contenido=\"%s\"\n",
               nuevo->id, nuevo->contenido);
        // Aviso en log
        guardar_log("COLA_LLENA id_mensaje=%d",
                    nuevo->id);
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

// ---------------------------------------------------------
// a) inicializar 10 grupos fijos
void inicializar_grupos() {
    pthread_mutex_lock(&grupos_mutex);
    for (int i = 0; i < MAX_GRUPOS; i++) {
        snprintf(grupos[i].nombre, sizeof(grupos[i].nombre), "grupo%d", i+1);
        grupos[i].count = 0;
        grupos[i].offset = 0;
        pthread_mutex_init(&grupos[i].mutex, NULL);
    }
    num_grupos = MAX_GRUPOS;
    pthread_mutex_unlock(&grupos_mutex);
}

// b) seleccionar el grupo con menos consumers
static GrupoConsumers *seleccionar_grupo_automatico() {
    pthread_mutex_lock(&grupos_mutex);
    int min_idx = 0;
    int min_count = grupos[0].count;
    for (int i = 1; i < num_grupos; i++) {
        if (grupos[i].count < min_count) {
            min_count = grupos[i].count;
            min_idx = i;
        }
    }
    GrupoConsumers *g = &grupos[min_idx];
    pthread_mutex_unlock(&grupos_mutex);
    return g;
}

// Busca un grupo por nombre (ninguno se crea dinámicamente, ya están los 10 precargados)
static GrupoConsumers *obtener_o_crear_grupo(const char *nombre) {
    pthread_mutex_lock(&grupos_mutex);
    for (int i = 0; i < num_grupos; i++) {
        if (strcmp(grupos[i].nombre, nombre) == 0) {
            pthread_mutex_unlock(&grupos_mutex);
            return &grupos[i];
        }
    }
    pthread_mutex_unlock(&grupos_mutex);
    return NULL;
}

void agregar_consumer_grupo(int sockfd, const char *grupo) {
    GrupoConsumers *g = obtener_o_crear_grupo(grupo);
    if (!g) return;
    pthread_mutex_lock(&g->mutex);
    if (g->count < MAX_CONSUMERS_PER_GROUP) {
        g->sockets[g->count++] = sockfd;
    }
    pthread_mutex_unlock(&g->mutex);
}

void quitar_consumer_grupo(int sockfd, const char *grupo) {
    pthread_mutex_lock(&grupos_mutex);
    for (int i = 0; i < num_grupos; i++) {
        if (strcmp(grupos[i].nombre, grupo) == 0) {
            GrupoConsumers *g = &grupos[i];
            pthread_mutex_lock(&g->mutex);
            for (int j = 0; j < g->count; j++) {
                if (g->sockets[j] == sockfd) {
                    // desplazamos el array
                    memmove(&g->sockets[j],
                            &g->sockets[j+1],
                            (g->count - j - 1) * sizeof(int));
                    g->count--;
                    // log y terminal: consumidor desconectado
                    guardar_log("CONSUMIDOR[%s] fd=%d desconectado",
                                grupo, sockfd);
                    printf("Consumer[%s] disconnected: fd=%d\n",
                           grupo, sockfd);
                    // si el grupo quedó sin consumidores
                    if (g->count == 0) {
                        guardar_log("GRUPO[%s] cerrado (sin consumers)", grupo);
                        printf("Group[%s] closed (no more consumers)\n", grupo);
                    }
                    break;
                }
            }
            pthread_mutex_unlock(&g->mutex);
            break;
        }
    }
    pthread_mutex_unlock(&grupos_mutex);
}

static int enviar_a_todos_grupos(Mensajillo *msg) {
    pthread_mutex_lock(&grupos_mutex);
    int sent_groups = 0;
    for (int i = 0; i < num_grupos; i++) {
        GrupoConsumers *g = &grupos[i];
        pthread_mutex_lock(&g->mutex);
        if (g->count > 0) {
            int idx = g->offset++ % g->count;
            int cs  = g->sockets[idx];
            if (send(cs, msg, sizeof(*msg), 0) != sizeof(*msg)) {
                close(cs);
                quitar_consumer_grupo(cs, g->nombre);
            } else {
                sent_groups++;
                guardar_log("GRUPO=%s ENVIADO consumer fd=%d id_mensaje=%d",
                            g->nombre, cs, msg->id);
            }
        }
        pthread_mutex_unlock(&g->mutex);
    }
    pthread_mutex_unlock(&grupos_mutex);
    return sent_groups;
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

#define MAX_MENSAJES_LOG 10000

static Mensajillo log_mensajes[MAX_MENSAJES_LOG];
static int num_log_mensajes = 0;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

// comparador para qsort
static int _cmp_entry(const void *a, const void *b) {
    const Mensajillo *x = a;
    const Mensajillo *y = b;
    return x->id - y->id;
}

static void actualizar_mensajes_log(const Mensajillo *msg) {
    Mensajillo *arr = NULL;
    size_t n = 0, cap = 0;
    FILE *f = fopen("mensajes.log", "r");
    if (f) {
        char line[512];
        while (fgets(line, sizeof(line), f)) {
            Mensajillo tmp;
            if (sscanf(line, "id_mensaje=%d contenido=\"%255[^\"]\"",
                       &tmp.id, tmp.contenido) == 2)
            {
                if (n == cap) {
                    cap = cap ? cap * 2 : 16;
                    arr = realloc(arr, cap * sizeof(*arr));
                }
                arr[n++] = tmp;
            }
        }
        fclose(f);
    }
    // añadir el mensaje nuevo
    if (n == cap) {
        cap = cap ? cap * 2 : 16;
        arr = realloc(arr, cap * sizeof(*arr));
    }
    arr[n++] = *msg;

    // ordenar por id
    qsort(arr, n, sizeof(*arr), _cmp_entry);

    // reescribir en modo "w" (sobrescribe) ya ordenado
    f = fopen("mensajes.log", "w");
    if (!f) {
        perror("abrir mensajes.log");
        free(arr);
        return;
    }
    for (size_t i = 0; i < n; i++) {
        fprintf(f, "id_mensaje=%d contenido=\"%s\"\n",
                arr[i].id, arr[i].contenido);
    }
    fclose(f);
    free(arr);
}

// Inicializa mensaje_id_global leyendo el mayor id en mensajes.log
static void inicializar_id_global() {
    FILE *f = fopen("mensajes.log", "r");
    if (!f) {
        mensaje_id_global = 1;
        return;
    }
    int max_id = 0;
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        int id;
        if (sscanf(line, "id_mensaje=%d ", &id) == 1 && id > max_id) {
            max_id = id;
        }
    }
    fclose(f);
    mensaje_id_global = max_id + 1;
}

// Hilo para manejar cada conexión
void *atender_cliente(void *arg) {
    int clientfd = *(int*)arg;
    free(arg);

    char buffer[sizeof(Mensajillo)];
    ssize_t n = recv(clientfd, buffer, sizeof(Mensajillo), MSG_PEEK);
    if (n <= 0) {
        close(clientfd);
        pthread_exit(NULL);
    }

    // rama producer
    if (n >= (ssize_t)sizeof(Mensajillo)) {
        // Producer conectado
        guardar_log("PRODUCER_CONEX fd=%d", clientfd);
        printf("Producer connected: fd=%d\n", clientfd);

        Mensajillo recibido;
        recv(clientfd, &recibido, sizeof(recibido), 0);

        // 1) asignar ID
        pthread_mutex_lock(&id_mutex);
        recibido.id = mensaje_id_global++;
        pthread_mutex_unlock(&id_mutex);

        // <-- aquí insertamos la actualización de mensajes.log
        actualizar_mensajes_log(&recibido);

        // 2) LOG de recepción **antes** de reenviar
        {
            struct sockaddr_in sa; socklen_t salen = sizeof(sa);
            if (getpeername(clientfd, (struct sockaddr*)&sa, &salen) == 0) {
                char hip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &sa.sin_addr, hip, sizeof(hip));
                int hport = ntohs(sa.sin_port);
                guardar_log("RECIBIDO producer %s:%d id_mensaje=%d ",
                            hip, hport, recibido.id);
            }
        }

        // 3) insertar en cola, reenviar y liberar espacio
        if (insertar_mensajillo(cola, &recibido)) {
            int enviados = enviar_a_todos_grupos(&recibido);
            // Aviso en terminal
            printf("MENSAJE_REENVIADO id_mensaje=%d a %d grupos\n",
                   recibido.id, enviados);
            // Aviso en log
            guardar_log("MENSAJE_REENVIADO id_mensaje=%d a %d grupos",
                        recibido.id, enviados);

            // ahora eliminamos el mensaje de la cola para liberar espacio
            Mensajillo descartado;
            if (!consumir_mensajillo(cola, &descartado)) {
                fprintf(stderr, "Error: no se pudo liberar mensaje de la cola\n");
            }
        }

        close(clientfd);
        pthread_exit(NULL);
    }

    // rama consumer
    n = recv(clientfd, buffer, sizeof(buffer), 0);
    if (n > 0 && strncmp(buffer, "CONSUMIR", 8) == 0) {
        char group_name[32] = {0};
        // si no viene nombre, elegir automáticamente
        if (sscanf(buffer, "CONSUMIR %31s", group_name) != 1) {
            GrupoConsumers *g = seleccionar_grupo_automatico();
            if (g) {
                strncpy(group_name, g->nombre, sizeof(group_name)-1);
            } else {
                strncpy(group_name, "grupo1", sizeof(group_name)-1);
            }
        }
        agregar_consumer_grupo(clientfd, group_name);
        guardar_log("CONSUMIDOR[%s] fd=%d conectado", group_name, clientfd);
        printf("Consumer[%s] connected: fd=%d\n", group_name, clientfd);

        // procesar ACKs…
        while (1) {
            char ackbuf[64];
            ssize_t r = recv(clientfd, ackbuf, sizeof(ackbuf), 0);
            if (r <= 0) {
                quitar_consumer_grupo(clientfd, group_name);
                guardar_log("CONSUMIDOR[%s] fd=%d desconectado", group_name, clientfd);
                break;
            }
            if (strncmp(ackbuf, "ACK ", 4) == 0) {
                int ack_id = atoi(ackbuf + 4);
                guardar_log("RECIBIDO_ACK consumer[%s] fd=%d id_mensaje=%d",
                            group_name, clientfd, ack_id);
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
    inicializar_grupos();
    inicializar_id_global();

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