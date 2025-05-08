# Sistema Distribuido de Broker de Mensajes (Mini-Kafka)

## Descripción del sistema implementado

Este proyecto implementa un sistema distribuido de mensajería inspirado en Apache Kafka, donde múltiples productores y consumidores se comunican a través de un broker central.

- **Productores:** envían mensajes al broker usando sockets TCP.
- **Broker:** recibe los mensajes, les asigna un ID único, los almacena temporalmente en una cola circular protegida por mutex, los registra en un log persistente y los distribuye a los consumidores organizados en grupos.
- **Consumidores:** se conectan al broker, se asignan a un grupo, y reciben mensajes. Cada grupo recibe una copia de cada mensaje, pero solo un consumidor dentro del grupo lo procesa, logrando balanceo de carga.

- El broker soporta múltiples conexiones concurrentes usando un thread pool para productores y polling para consumidores.
- Los logs de mensajes y eventos se almacenan en archivos persistentes (`mensajes.log` y `broker.log`).

## Instrucciones para compilar y ejecutar

### Compilación

Desde la raíz del proyecto, ejecuta:

```sh
make
```

Esto generará los ejecutables en la carpeta `src/`.

### Ejecución

Desde la carpeta `src/`, ejecuta en terminales separadas según el rol:

```sh
./broker
./producer
./consumer
```

Puedes lanzar múltiples productores y consumidores al mismo tiempo.

## Estrategia utilizada para evitar interbloqueos

- **Mutexes por recurso:** Cada estructura compartida (cola, logs, grupos, lista de consumidores) tiene su propio mutex, evitando bloqueos globales.
- **Orden fijo de adquisición de locks:** Cuando es necesario tomar más de un lock, se respeta siempre el mismo orden para evitar ciclos de espera.
- **Thread pool para productores:** Los productores son atendidos por un pool de hilos, evitando la creación excesiva de threads y el agotamiento de recursos.
- **Polling para consumidores:** Los consumidores son gestionados mediante polling periódico, permitiendo detectar desconexiones y liberar recursos rápidamente.
- **Timeouts y reintentos:** Operaciones potencialmente bloqueantes (como inserción en colas llenas) usan reintentos y timeouts, evitando bloqueos indefinidos.
- **Secciones críticas pequeñas:** El tiempo que un hilo mantiene un mutex es mínimo, reduciendo la probabilidad de interbloqueos.

## Problemas conocidos o limitaciones

- **Persistencia limitada:** Si el broker se reinicia, solo se conserva el ID global de mensajes leyendo el log, pero los mensajes en la cola se pierden.
- **Tamaño de la cola limitado:** Si la cola de mensajes se llena, los productores pueden experimentar rechazos o demoras.
- **Escalabilidad limitada:** El sistema está diseñado para ejecutarse en una sola máquina y no soporta múltiples brokers ni balanceo de carga entre ellos.
- **No hay soporte para mensajes de gran tamaño:** El tamaño máximo de cada mensaje está limitado a 256 bytes (`MAXIMO_MENSAJE`).
- **Distribución simple a grupos:** El reparto de mensajes a los grupos es round-robin y no garantiza balanceo perfecto si los consumers se conectan/desconectan frecuentemente.
- **No hay autenticación ni cifrado:** La comunicación es local y sin seguridad adicional.
- **Logs pueden crecer indefinidamente:** Los archivos `broker.log` y `mensajes.log` pueden crecer mucho bajo carga intensa.
- **No hay control de errores avanzado:** Si un consumer o producer se comporta de forma inesperada, puede causar desconexiones abruptas.
- **No se implementa offset persistente por consumidor:** Los offsets se manejan en memoria y no se recuperan tras un reinicio.

## Ejemplo de uso

1. Abre una terminal y ejecuta el broker:
    ```sh
    cd src
    ./broker
    ```
2. En otras terminales, ejecuta uno o varios consumidores:
    ```sh
    ./consumer
    ```
3. En otras terminales adicionales, ejecuta uno o varios productores:
    ```sh
    ./producer
    ```
4. Observa los logs generados en `mensajes.log` y `broker.log`.

---