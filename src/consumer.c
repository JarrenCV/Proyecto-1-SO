#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define BROKER_IP "127.0.0.1"
#define BROKER_PORT 5000
#define MAXIMO_MENSAJE 256

typedef struct {
    int id;
    char contenido[MAXIMO_MENSAJE];
} Mensajillo;

int main() {
    const char peticion[] = "CONSUMIR";

    int sockfd = -1;
    struct sockaddr_in broker_addr;
    Mensajillo mensaje;

    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_PORT);
    if (inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr) <= 0) {
        perror("inet_pton");
        exit(EXIT_FAILURE);
    }

    while (1) {
        if (sockfd < 0) {
            if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                perror("socket");
                //sleep(1);
                continue;
            }
            if (connect(sockfd, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
                perror("connect");
                close(sockfd);
                sockfd = -1;
                //sleep(1);
                continue;
            }
        }

        if (send(sockfd, peticion, strlen(peticion) + 1, 0) < 0) {
            perror("send");
            close(sockfd);
            sockfd = -1;
            //sleep(1);
            continue;
        }

        ssize_t n = recv(sockfd, &mensaje, sizeof(Mensajillo), 0);
        if (n == sizeof(Mensajillo)) {
            printf("[Consumer PID=%d] Recibido: id_mensaje=%d, contenido=%s\n",
                   getpid(), mensaje.id, mensaje.contenido);

            char ackmsg[32];
            snprintf(ackmsg, sizeof(ackmsg), "ACK %d", mensaje.id);
            send(sockfd, ackmsg, strlen(ackmsg) + 1, 0);
        } else if (n == 0) {
            printf("[Consumer PID=%d] Broker cerró la conexión.\n", getpid());
            close(sockfd);
            sockfd = -1;
        } else {
            printf("[Consumer PID=%d] No hay mensajes disponibles o error.\n", getpid());
        }

        //sleep(1);
    }

    if (sockfd >= 0) close(sockfd);
    return 0;
}