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

#define MAXIMO_MENSAJE 256
#define TAMANO_COLA 10
#define SHM_KEY 0x1234

typedef struct {
    char contenido[MAXIMO_MENSAJE];
} Mensajillo;

typedef struct {
    Mensajillo messages[TAMANO_COLA];
    int pleer;
    int plibre;
    pthread_mutex_t mutexCola;
} ColaMensajillos;

int estaRellenita(ColaMensajillos *cola) {
    return (cola->plibre + 1) % TAMANO_COLA == cola->pleer;
}

int noTieneElementos(ColaMensajillos *cola) {
    return cola->pleer == cola->plibre;
}

void insertar_mensajillo(ColaMensajillos *cola, Mensajillo *nuevo) {
    pthread_mutex_lock(&cola->mutexCola); // INICIO sección crítica
    if (!estaRellenita(cola)) {
        cola->messages[cola->plibre] = *nuevo;
        cola->plibre = (cola->plibre + 1) % TAMANO_COLA;
    } else {
        printf("Colilla rellenita, no cabe más\n");
    }
    pthread_mutex_unlock(&cola->mutexCola); // FIN sección crítica
}

int consumir_mensajillo(ColaMensajillos *cola, Mensajillo *destino) {
    int resultado = 0;
    pthread_mutex_lock(&cola->mutexCola); // INICIO sección crítica
    if (!noTieneElementos(cola)) {
        *destino = cola->messages[cola->pleer];
        cola->pleer = (cola->pleer + 1) % TAMANO_COLA;
        resultado = 1; // Éxito
    } else {
        printf("Colilla sin elementos, no hay nada que consumir\n");
        resultado = 0;
    }
    pthread_mutex_unlock(&cola->mutexCola); // FIN sección crítica
    return resultado;
}

void guardar_en_logillo(Mensajillo *mensaje) {
    FILE *log = fopen("mensajes.log", "a");
    if (log != NULL) {
        fprintf(log, "%s\n", mensaje->contenido);
        fclose(log);
    } else {
        printf("No se pudo abrir el archivillo\n");
    }
}

void inicializar(ColaMensajillos *cola) {
    // Inicializa los índices de la cola circular
    cola->pleer = 0;
    cola->plibre = 0;

    // Inicializa los atributos del mutex para que sea compartido entre procesos
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);

    // Inicializa el mutex de la cola con los atributos configurados
    pthread_mutex_init(&cola->mutexCola, &attr);

    // Libera los recursos de los atributos del mutex
    pthread_mutexattr_destroy(&attr);
}

int main() {
    int fd = shm_open("/cola_mensajes", O_CREAT | O_RDWR, 0666);
    if (fd == -1) {
        perror("shm_open");
        exit(1);
    }

    // Ajustar el tamaño del segmento
    if (ftruncate(fd, sizeof(ColaMensajillos)) == -1) {
        perror("ftruncate");
        exit(1);
    }

    // Mapear la memoria compartida
    ColaMensajillos *cola = mmap(NULL, sizeof(ColaMensajillos),
                                 PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (cola == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }

    // Inicializar la cola solo la primera vez
    inicializar_cola(cola);

    printf("Cola de mensajes en memoria compartida inicializada.\n");

    // Aquí iría el loop principal del broker (aceptar conexiones, etc.)

    // Al finalizar, liberar la memoria compartida
    munmap(cola, sizeof(ColaMensajillos));
    close(fd);
    // shm_unlink("/cola_mensajes"); // Descomentar si quieres eliminar el segmento al terminar

    return 0;
}

#endif