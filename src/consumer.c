#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdarg.h>

#define BROKER_IP "127.0.0.1"
#define BROKER_PORT 5000
#define MAXIMO_MENSAJE 256

typedef struct {
    int id;
    char contenido[MAXIMO_MENSAJE];
} Mensajillo;

int main() {
    int sockfd = -1;
    struct sockaddr_in broker_addr;
    Mensajillo mensaje;
    char peticion[] = "CONSUMIR";

    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_PORT);
    if (inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr) <= 0) {
        perror("inet_pton");
        exit(EXIT_FAILURE);
    }

    while (1) {
        // Si el socket no está abierto, intenta abrirlo y conectarlo
        if (sockfd < 0) {
            if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                perror("socket");
                sleep(1);
                continue;
            }
            if (connect(sockfd, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
                perror("connect");
                close(sockfd);
                sockfd = -1;
                sleep(1);
                continue;
            }
        }

        // Enviar petición de consumo
        if (send(sockfd, peticion, sizeof(peticion), 0) < 0) {
            perror("send");
            close(sockfd);
            sockfd = -1;
            sleep(1);
            continue;
        }

        // Intentar recibir un mensaje
        ssize_t n = recv(sockfd, &mensaje, sizeof(Mensajillo), 0);
        if (n == sizeof(Mensajillo)) {
            printf("[Consumer PID=%d] Recibido: id=%d, contenido=%s\n",
                   getpid(), mensaje.id, mensaje.contenido);

            // Envía ACK al broker para que lo loguee él
            char ackmsg[32];
            snprintf(ackmsg, sizeof(ackmsg), "ACK %d", mensaje.id);
            send(sockfd, ackmsg, strlen(ackmsg) + 1, 0);

            // no guardamos log local, todo va al broker
        } else if (n == 0) {
            // El broker cerró la conexión
            printf("[Consumer PID=%d] Broker cerró la conexión.\n", getpid());
            close(sockfd);
            sockfd = -1;
        } else {
            printf("[Consumer PID=%d] No hay mensajes disponibles o error.\n", getpid());
        }

        sleep(1);
    }

    // Nunca se llega aquí, pero por buenas prácticas:
    if (sockfd >= 0) close(sockfd);
    return 0;
}