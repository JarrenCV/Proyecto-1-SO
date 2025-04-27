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
    int sockfd;
    struct sockaddr_in broker_addr;
    Mensajillo mensaje;

    // Mensaje fijo
    snprintf(mensaje.contenido, MAXIMO_MENSAJE, "Hola, soy el producer con PID %d", getpid());
    mensaje.id = 0; // El broker asignará el ID

    // Crear socket
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    memset(&broker_addr, 0, sizeof(broker_addr));

    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_PORT);
    if (inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    // Conectar al broker
    if (connect(sockfd, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("connect");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    // Enviar el Mensajillo
    if (send(sockfd, &mensaje, sizeof(Mensajillo), 0) < 0) {
        perror("send");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    printf("Mensaje enviado al broker: producer con ID=%d, Contenido=%s\n", getpid(), mensaje.contenido);

    close(sockfd);
    return 0;
}