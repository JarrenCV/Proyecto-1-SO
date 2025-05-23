#ifndef BROKER_H
#define BROKER_H

#define _POSIX_C_SOURCE 199309L
#define MAX_MENSAJES_LOG 10000
#define MAX_GRUPOS 3
#define MAXIMO_MENSAJE 256
#define TAMANO_COLA 100
#define BROKER_PORT 5000
#define THREAD_POOL_SIZE 8
#define MAX_QUEUE_SIZE 256

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>
#include <stdarg.h>
#include <limits.h>
#include <stdbool.h>
#include <poll.h>

typedef struct {
    int sockfd;
    char grupo[32];
} ConsumerSocketInfo;

typedef struct {
    int id;
    char contenido[MAXIMO_MENSAJE];
} Mensajillo;

// Datos para el manejador de clientes (para producers)
typedef struct {
    int clientfd;
} ClientHandlerData;

// Estructura para hilos dedicados de consumers
typedef struct {
    int clientfd;
} ConsumerThreadData;

// Datos para actualizar logs
typedef struct {
    Mensajillo mensaje;
} LogUpdateData;

// Estructura para tareas en el thread pool
typedef enum {
    TASK_CLIENT_HANDLER,
    TASK_LOG_UPDATE,
    TASK_CONSUMERS_POLL,
} TaskType;

// Estructura para tareas
typedef struct {
    TaskType type;
    void *data;
} Task;

// Estructura para thread pool
typedef struct {
    pthread_t threads[THREAD_POOL_SIZE];
    Task queue[MAX_QUEUE_SIZE];
    int queue_size;
    int front;
    int rear;
    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_not_empty;
    pthread_cond_t queue_not_full;
    bool shutdown;
} ThreadPool;

typedef struct {
    Mensajillo messages[TAMANO_COLA];
    int pleer;
    int plibre;
    pthread_mutex_t mutexCola;
} ColaMensajillos;

typedef struct {
    char nombre[32];
    int *sockets;
    int count;
    int capacity;
    int offset;
    pthread_mutex_t mutex;
} GrupoConsumers;

// Variables globales
ColaMensajillos *cola = NULL;
int mensaje_id_global = 1;
pthread_mutex_t id_mutex = PTHREAD_MUTEX_INITIALIZER;
static GrupoConsumers grupos[MAX_GRUPOS];
static int num_grupos = 0;
static pthread_mutex_t grupos_mutex = PTHREAD_MUTEX_INITIALIZER;
static ThreadPool *pool = NULL;
static FILE *f_broker_log = NULL;
static FILE *f_mensajes_log = NULL;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t log_sorter_thread;
static volatile int should_sort_log = 0;
static pthread_mutex_t sort_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sort_cond = PTHREAD_COND_INITIALIZER;
static volatile int log_sorter_running = 1;
static ConsumerSocketInfo *consumers = NULL;
static int num_consumers = 0;
static int consumers_capacity = 0;
static pthread_mutex_t consumers_mutex = PTHREAD_MUTEX_INITIALIZER;

// Prototipos de funciones
void atender_cliente_task(ClientHandlerData *data);
void *consumer_handler_thread(void *arg);
int estaRellenita(ColaMensajillos *cola);
int noTieneElementos(ColaMensajillos *cola);
int insertar_mensajillo(ColaMensajillos *cola, Mensajillo *nuevo);
int consumir_mensajillo(ColaMensajillos *cola, Mensajillo *destino);
void inicializar_cola(ColaMensajillos *cola);
void inicializar_grupos();
static GrupoConsumers *seleccionar_grupo_automatico();
static GrupoConsumers *obtener_o_crear_grupo(const char *nombre);
void agregar_consumer_grupo(int sockfd, const char *grupo);
void quitar_consumer_grupo(int sockfd, const char *grupo);
static int enviar_a_todos_grupos(Mensajillo *msg);
static void guardar_en_logillo(Mensajillo* mensaje);
static void inicializar_id_global();
void inicializar_log_sorter();
void finalizar_log_sorter();
void close_log_files();
void atender_consumers_poll_task(void *unused);
static void actualizar_mensajes_log(const Mensajillo *msg);
static void guardar_log(const char *fmt, ...);

// Inicialización del thread pool
ThreadPool* thread_pool_init() {
    ThreadPool* pool = (ThreadPool*)malloc(sizeof(ThreadPool));
    if (!pool) {
        perror("malloc thread pool");
        return NULL;
    }
    
    pool->queue_size = 0;
    pool->front = 0;
    pool->rear = 0;
    pool->shutdown = false;
    
    pthread_mutex_init(&pool->queue_mutex, NULL);
    pthread_cond_init(&pool->queue_not_empty, NULL);
    pthread_cond_init(&pool->queue_not_full, NULL);
    
    return pool;
}

// Añadir tarea al thread pool
int thread_pool_add_task(ThreadPool* pool, TaskType type, void* data) {
    pthread_mutex_lock(&pool->queue_mutex);
    
    // Esperar si la cola está llena
    while (pool->queue_size == MAX_QUEUE_SIZE && !pool->shutdown) {
        pthread_cond_wait(&pool->queue_not_full, &pool->queue_mutex);
    }
    
    // No aceptar más tareas si está en shutdown
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->queue_mutex);
        return -1;
    }
    
    // Añadir tarea a la cola
    Task task = {
        .type = type,
        .data = data
    };
    
    pool->queue[pool->rear] = task;
    pool->rear = (pool->rear + 1) % MAX_QUEUE_SIZE;
    pool->queue_size++;
    
    // Señalar que la cola ya no está vacía
    pthread_cond_signal(&pool->queue_not_empty);
    pthread_mutex_unlock(&pool->queue_mutex);
    
    return 0;
}

// Función que ejecutan los threads del pool
void* thread_function(void* arg) {
    ThreadPool* pool = (ThreadPool*)arg;
    
    while (1) {
        pthread_mutex_lock(&pool->queue_mutex);
        
        // Esperar si la cola está vacía y no hay shutdown
        while (pool->queue_size == 0 && !pool->shutdown) {
            pthread_cond_wait(&pool->queue_not_empty, &pool->queue_mutex);
        }
        
        // Si es shutdown y la cola está vacía, terminar
        if (pool->shutdown && pool->queue_size == 0) {
            pthread_mutex_unlock(&pool->queue_mutex);
            pthread_exit(NULL);
        }
        
        // Obtener una tarea de la cola
        Task task = pool->queue[pool->front];
        pool->front = (pool->front + 1) % MAX_QUEUE_SIZE;
        pool->queue_size--;
        
        // Señalar que la cola ya no está llena
        pthread_cond_signal(&pool->queue_not_full);
        pthread_mutex_unlock(&pool->queue_mutex);
        
        // Ejecutar la tarea según su tipo
        switch (task.type) {
            case TASK_CLIENT_HANDLER: {
                ClientHandlerData* data = (ClientHandlerData*)task.data;
                atender_cliente_task(data);
                break;
            }
            case TASK_LOG_UPDATE: {
                LogUpdateData* data = (LogUpdateData*)task.data;
                actualizar_mensajes_log(&data->mensaje);
                free(data);
                break;
            }
            case TASK_CONSUMERS_POLL: {
                atender_consumers_poll_task(task.data);
                break;
            }
        }
    }
    
    return NULL;
}

// Iniciar los threads del pool
int thread_pool_start(ThreadPool* pool) {
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        if (pthread_create(&pool->threads[i], NULL, thread_function, pool) != 0) {
            perror("pthread_create");
            return -1;
        }
    }
    return 0;
}

// Cerrar el thread pool
void thread_pool_shutdown(ThreadPool* pool) {
    if (!pool) return;
    
    pthread_mutex_lock(&pool->queue_mutex);
    pool->shutdown = true;
    pthread_cond_broadcast(&pool->queue_not_empty);
    pthread_cond_broadcast(&pool->queue_not_full);
    pthread_mutex_unlock(&pool->queue_mutex);
    
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    
    pthread_mutex_destroy(&pool->queue_mutex);
    pthread_cond_destroy(&pool->queue_not_empty);
    pthread_cond_destroy(&pool->queue_not_full);
    
    free(pool);
}

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
    int retries = 5;  // Intentaremos varias veces antes de fallar
    
    while (retries > 0) {
        pthread_mutex_lock(&cola->mutexCola);
        if (!estaRellenita(cola)) {
            // Hay espacio, insertar
            cola->messages[cola->plibre] = *nuevo;
            cola->plibre = (cola->plibre + 1) % TAMANO_COLA;
            pthread_mutex_unlock(&cola->mutexCola);
            return 1;  // Éxito
        }
        pthread_mutex_unlock(&cola->mutexCola);
        
        // Cola llena, esperar un poco e intentar de nuevo
        fprintf(stderr, "Cola llena. Esperando para mensaje id=%d (intento %d)\n", 
                nuevo->id, 6-retries);
        usleep(100000);  // 100ms
        retries--;
    }
    
    fprintf(stderr, "COLA_LLENA id_mensaje=%d contenido=\"%s\"\n", 
            nuevo->id, nuevo->contenido);
    return 0;  // La cola sigue llena después de varios intentos
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

// a) inicializar 10 grupos fijos
void inicializar_grupos() {
    pthread_mutex_lock(&grupos_mutex);
    for (int i = 0; i < MAX_GRUPOS; i++) {
        snprintf(grupos[i].nombre, sizeof(grupos[i].nombre), "grupo%d", i+1);
        grupos[i].count = 0;
        grupos[i].capacity = 16;
        grupos[i].sockets = malloc(grupos[i].capacity * sizeof(int));
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
    if (g->count >= g->capacity) {
        int newcap = g->capacity * 2;
        int *tmp = realloc(g->sockets, newcap * sizeof(int));
        if (tmp) {
            g->sockets = tmp;
            g->capacity = newcap;
        } else {
            pthread_mutex_unlock(&g->mutex);
            return;
        }
    }
    g->sockets[g->count++] = sockfd;
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

// Mutex global para proteger la escritura del log
static pthread_mutex_t log_file_mutex = PTHREAD_MUTEX_INITIALIZER;

static void guardar_log(const char *fmt, ...) {
    static FILE *f = NULL;  // Mantener el archivo abierto
    
    pthread_mutex_lock(&log_file_mutex);
    
    // Abrir el archivo solo una vez
    if (!f) {
        f = fopen("broker.log", "a");
        if (!f) {
            perror("abrir broker.log");
            pthread_mutex_unlock(&log_file_mutex);
            return;
        }
        // Configurar para escritura sin buffer
        setvbuf(f, NULL, _IONBF, 0);
    }
    
    va_list ap;
    va_start(ap, fmt);
    vfprintf(f, fmt, ap);
    fprintf(f, "\n");
    va_end(ap);
    
    // No llamamos a fsync aquí para mejorar rendimiento
    // fflush es suficiente para la mayoría de casos
    fflush(f);
    
    // No cerramos el archivo para evitar la sobrecarga de apertura/cierre
    pthread_mutex_unlock(&log_file_mutex);
}

// Comparador para qsort
static int _cmp_entry(const void *a, const void *b) {
    const Mensajillo *x = a;
    const Mensajillo *y = b;
    return x->id - y->id;
}

// Función que reordena periódicamente los mensajes
void* log_sorter_function(void* arg) {
    while (log_sorter_running) {
        // Esperar señal o timeout
        pthread_mutex_lock(&sort_mutex);
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 5; // Reordenar cada 5 segundos o cuando se solicite
        
        // Esperar por señal o timeout
        int was_signaled = 0;
        while (!should_sort_log && log_sorter_running) {
            int rv = pthread_cond_timedwait(&sort_cond, &sort_mutex, &ts);
            if (rv == ETIMEDOUT)
                break;
            else if (rv == 0)
                was_signaled = 1;
        }
        
        if (should_sort_log || (!was_signaled && log_sorter_running)) {
            should_sort_log = 0; // Reiniciar bandera
            pthread_mutex_unlock(&sort_mutex);
            
            // Reordenar archivo - usamos mutex_log para bloquear actualizaciones mientras reordenamos
            pthread_mutex_lock(&log_mutex);
            
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
                            if (!arr) {
                                fprintf(stderr, "Error de memoria al reordenar log\n");
                                break;
                            }
                        }
                        arr[n++] = tmp;
                    }
                }
                fclose(f);
                
                if (arr && n > 0) {
                    // Ordenar por ID
                    qsort(arr, n, sizeof(*arr), _cmp_entry);
                    
                    // Escribir archivo ordenado
                    f = fopen("mensajes.log", "w");
                    if (f) {
                        for (size_t i = 0; i < n; i++) {
                            fprintf(f, "id_mensaje=%d contenido=\"%s\"\n",
                                   arr[i].id, arr[i].contenido);
                        }
                        fflush(f);
                        fsync(fileno(f));
                        fclose(f);
                    }
                    free(arr);
                }
            }
            
            pthread_mutex_unlock(&log_mutex);
        } else {
            pthread_mutex_unlock(&sort_mutex);
        }
    }
    
    return NULL;
}

// Modificaciones a la función actualizar_mensajes_log
static void actualizar_mensajes_log(const Mensajillo *msg) {
    static FILE *f = NULL;  // Mantener el archivo abierto
    
    pthread_mutex_lock(&log_mutex);
    
    // Abrir el archivo solo una vez
    if (!f) {
        f = fopen("mensajes.log", "a");
        if (!f) {
            perror("abrir mensajes.log para append");
            pthread_mutex_unlock(&log_mutex);
            return;
        }
        // Configurar para escritura sin buffer
        setvbuf(f, NULL, _IONBF, 0);
    }
    
    fprintf(f, "id_mensaje=%d contenido=\"%s\"\n", msg->id, msg->contenido);
    fflush(f);
    
    // No cerramos el archivo para evitar la sobrecarga de apertura/cierre
    pthread_mutex_unlock(&log_mutex);
}

void close_log_files() {
    // Esta función debería llamarse antes del return en main()
    pthread_mutex_lock(&log_file_mutex);
    if (f_broker_log) {
        fclose(f_broker_log);
        f_broker_log = NULL;
    }
    pthread_mutex_unlock(&log_file_mutex);
    
    pthread_mutex_lock(&log_mutex);
    if (f_mensajes_log) {
        fclose(f_mensajes_log);
        f_mensajes_log = NULL;
    }
    pthread_mutex_unlock(&log_mutex);
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

// Función para atender al cliente producer (usando thread pool)
void atender_cliente_task(ClientHandlerData *data) {
    int clientfd = data->clientfd;
    free(data);
    
    // Solo procesa producers ahora
    Mensajillo recibido;
    recv(clientfd, &recibido, sizeof(recibido), 0);
    
    // 1) asignar ID
    pthread_mutex_lock(&id_mutex);
    recibido.id = mensaje_id_global++;
    pthread_mutex_unlock(&id_mutex);
    
    // 2) Delegamos la actualización del log a un thread del pool
    LogUpdateData *log_data = malloc(sizeof(LogUpdateData));
    if (log_data) {
        log_data->mensaje = recibido;
        thread_pool_add_task(pool, TASK_LOG_UPDATE, log_data);
    }
    
    // 3) LOG de recepción **antes** de reenviar
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
    
    // 4) insertar en cola, reenviar y liberar espacio
    if (insertar_mensajillo(cola, &recibido)) {
        int enviados = enviar_a_todos_grupos(&recibido);
        // Ahora eliminamos el mensaje de la cola para liberar espacio
        Mensajillo dummy;
        consumir_mensajillo(cola, &dummy);
        
        // Aviso en terminal
        printf("MENSAJE_REENVIADO id_mensaje=%d a %d grupos\n",
               recibido.id, enviados);
        // Aviso en log
        guardar_log("MENSAJE_REENVIADO id_mensaje=%d a %d grupos",
                    recibido.id, enviados);
    }
    
    close(clientfd);
}

// Nueva función para manejar consumers en hilos dedicados
void *consumer_handler_thread(void *arg) {
    ConsumerThreadData *data = (ConsumerThreadData *)arg;
    int clientfd = data->clientfd;
    free(data); // Liberar la memoria asignada para la estructura
    
    char buffer[256];
    ssize_t n = recv(clientfd, buffer, sizeof(buffer), 0);
    
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
        guardar_log("CONSUMIDOR[%s] fd=%d conectado (hilo dedicado)", group_name, clientfd);
        printf("Consumer[%s] connected: fd=%d (dedicated thread)\n", group_name, clientfd);
        
        // procesar ACKs…
        while (1) {
            char ackbuf[64];
            ssize_t r = recv(clientfd, ackbuf, sizeof(ackbuf), 0);
            if (r <= 0) {
                quitar_consumer_grupo(clientfd, group_name);
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
    
    return NULL;
}

void inicializar_log_sorter() {
    pthread_create(&log_sorter_thread, NULL, log_sorter_function, NULL);
}

void finalizar_log_sorter() {
    // Señalizar hilo para terminar
    pthread_mutex_lock(&sort_mutex);
    log_sorter_running = 0;
    pthread_cond_signal(&sort_cond);
    pthread_mutex_unlock(&sort_mutex);
    
    // Esperar a que termine
    pthread_join(log_sorter_thread, NULL);
}

void atender_consumers_poll_task(void *unused) {
    (void)unused;
    struct pollfd *pfds = malloc(num_consumers * sizeof(struct pollfd));
    if (!pfds) return;
    int nfds = 0;

    pthread_mutex_lock(&consumers_mutex);
    nfds = num_consumers;
    for (int i = 0; i < nfds; ++i) {
        pfds[i].fd = consumers[i].sockfd;
        pfds[i].events = POLLIN;
    }
    pthread_mutex_unlock(&consumers_mutex);

    int ret = poll(pfds, nfds, 50); // 50ms timeout
    if (ret > 0) {
        pthread_mutex_lock(&consumers_mutex);
        for (int i = 0; i < nfds; ++i) {
            if (pfds[i].revents & POLLIN) {
                Mensajillo msg;
                ssize_t n = recv(pfds[i].fd, &msg, sizeof(msg), MSG_PEEK);
                if (n <= 0) {
                    // Consumer desconectado
                    quitar_consumer_grupo(pfds[i].fd, consumers[i].grupo);
                    close(pfds[i].fd);
                    // Eliminar de la lista
                    consumers[i] = consumers[num_consumers-1];
                    num_consumers--;
                    i--; nfds--;
                    continue;
                }
                // Leer ACKs
                char ackbuf[64];
                n = recv(pfds[i].fd, ackbuf, sizeof(ackbuf), 0);
                if (n > 0 && strncmp(ackbuf, "ACK ", 4) == 0) {
                    int ack_id = atoi(ackbuf + 4);
                    guardar_log("RECIBIDO_ACK consumer[%s] fd=%d id_mensaje=%d",
                                consumers[i].grupo, pfds[i].fd, ack_id);
                }
            }
        }
        pthread_mutex_unlock(&consumers_mutex);
    }
    free(pfds);
}

void *consumers_poll_scheduler(void *arg) {
    (void)arg;
    while (1) {
        thread_pool_add_task(pool, TASK_CONSUMERS_POLL, NULL);
        usleep(50000); // 50ms
    }
    return NULL;
}

int main() {
    // Memoria compartida local
    cola = mmap(NULL, sizeof(ColaMensajillos), PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (cola == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }
    inicializar_cola(cola);
    inicializar_grupos();
    inicializar_id_global();
    inicializar_log_sorter();

    pool = thread_pool_init();
    if (!pool) {
        fprintf(stderr, "Error al inicializar el thread pool\n");
        exit(1);
    }
    
    // Iniciar los threads
    if (thread_pool_start(pool) != 0) {
        fprintf(stderr, "Error al iniciar los threads del pool\n");
        exit(1);
    }

    printf("Broker iniciado con %d threads en pool para producers. Esperando conexiones en el puerto %d...\n", 
           THREAD_POOL_SIZE, BROKER_PORT);

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

    if (listen(serverfd, 1024) < 0) {
        perror("listen");
        exit(1);
    }

    pthread_t poll_sched_thread;
    pthread_create(&poll_sched_thread, NULL, consumers_poll_scheduler, NULL);
    pthread_detach(poll_sched_thread);

    while (1) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int clientfd = accept(serverfd, (struct sockaddr *)&cli_addr, &cli_len);
        if (clientfd < 0) {
            perror("accept");
            continue;
        }
        
        // Determinar si es un producer o consumer antes de asignar hilos
        char buffer[sizeof(Mensajillo)];
        ssize_t n = recv(clientfd, buffer, sizeof(Mensajillo), MSG_PEEK);
        
        if (n <= 0) {
            close(clientfd);
            continue;
        }
        
        // Producer: usar el thread pool
        if (n >= (ssize_t)sizeof(Mensajillo)) {
            // Crear datos para la tarea
            ClientHandlerData *data = malloc(sizeof(ClientHandlerData));
            if (!data) {
                perror("malloc client data");
                close(clientfd);
                continue;
            }
            data->clientfd = clientfd;
            
            // Añadir tarea al thread pool
            if (thread_pool_add_task(pool, TASK_CLIENT_HANDLER, data) != 0) {
                fprintf(stderr, "Error al añadir tarea al thread pool\n");
                free(data);
                close(clientfd);
            }
        } 
        // Consumer: crear un hilo dedicado
        else if (n > 0 && strncmp(buffer, "CONSUMIR", 8) == 0) {
            char group_name[32] = {0};
            if (sscanf(buffer, "CONSUMIR %31s", group_name) != 1) {
                GrupoConsumers *g = seleccionar_grupo_automatico();
                if (g) strncpy(group_name, g->nombre, sizeof(group_name)-1);
                else strncpy(group_name, "grupo1", sizeof(group_name)-1);
            }
            agregar_consumer_grupo(clientfd, group_name);

            pthread_mutex_lock(&consumers_mutex);
            if (num_consumers >= consumers_capacity) {
                int newcap = consumers_capacity ? consumers_capacity * 2 : 128;
                ConsumerSocketInfo *tmp = realloc(consumers, newcap * sizeof(ConsumerSocketInfo));
                if (tmp) {
                    consumers = tmp;
                    consumers_capacity = newcap;
                } else {
                    close(clientfd);
                    printf("No hay memoria para más consumers\n");
                    pthread_mutex_unlock(&consumers_mutex);
                    continue;
                }
            }
            consumers[num_consumers].sockfd = clientfd;
            strncpy(consumers[num_consumers].grupo, group_name, sizeof(consumers[num_consumers].grupo)-1);
            num_consumers++;
            guardar_log("CONSUMIDOR[%s] fd=%d conectado (threadpool)", group_name, clientfd);
            printf("Consumer[%s] connected: fd=%d (threadpool)\n", group_name, clientfd);
            pthread_mutex_unlock(&consumers_mutex);
        }
        else {
            // Conexión no reconocida
            fprintf(stderr, "Conexión no reconocida, cerrando fd=%d\n", clientfd);
            close(clientfd);
        }
    }

    // Nunca llegará aquí, pero por completitud:
    close(serverfd);
    thread_pool_shutdown(pool);
    finalizar_log_sorter();
    munmap(cola, sizeof(ColaMensajillos));
    return 0;
}
#endif