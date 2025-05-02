#!/bin/bash

# Colores para mejor legibilidad
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuración de prueba
NUM_CONSUMERS=15           # Número total de consumers
NUM_PRODUCERS=3000          # Número de producers a lanzar
TEST_DURATION=30           # Duración total de la prueba en segundos

rm -f broker.log mensajes.log consumer_*.log broker consumer producer

# Compilar todos los programas
echo -e "${BLUE}Compilando los programas...${NC}"

gcc -o broker broker.c -lpthread
if [ $? -ne 0 ]; then
    echo -e "${RED}Error al compilar broker.c${NC}"
    exit 1
fi

gcc -o consumer consumer.c
if [ $? -ne 0 ]; then
    echo -e "${RED}Error al compilar consumer.c${NC}"
    exit 1
fi

gcc -o producer producer.c
if [ $? -ne 0 ]; then
    echo -e "${RED}Error al compilar producer.c${NC}"
    exit 1
fi

# Función para limpiar procesos al terminar
function cleanup {
    # Recopilar resultados
    echo -e "${BLUE}Recopilando resultados...${NC}"
    #sleep 5
    # Contar mensajes en el log de mensajes (este sí lo mantenemos)
    if [ -f mensajes.log ]; then
        NUM_MESSAGES=$(wc -l < mensajes.log)
        echo -e "${GREEN}Total de mensajes registrados: $NUM_MESSAGES${NC}"
        echo -e "${YELLOW}Primeros 5 mensajes:${NC}"
        head -n 5 mensajes.log
    else
        echo -e "${RED}No se encontró el archivo mensajes.log${NC}"
    fi


    echo -e "${YELLOW}Terminando todos los procesos...${NC}"
    pkill -P $$  # Mata todos los procesos hijos de este script
    # También aseguramos que broker se cierre
    pkill -f "./broker"
    echo -e "${GREEN}Limpieza completa${NC}"
    exit 0
}

# Registrar la función de limpieza para cuando el script termine
trap cleanup EXIT INT TERM



# Iniciar el broker (redireccionando salida a /dev/null)
echo -e "${BLUE}Iniciando el broker...${NC}"
./broker > /dev/null 2>&1 &
BROKER_PID=$!

# Esperar a que el broker esté listo (ajustar según sea necesario)
sleep 2

# Verificar que el broker está ejecutándose
if ! ps -p $BROKER_PID > /dev/null; then
    echo -e "${RED}El broker no pudo iniciarse.${NC}"
    exit 1
fi

echo -e "${GREEN}Broker iniciado con PID $BROKER_PID${NC}"

# Iniciar consumers (también redireccionando salida)
echo -e "${BLUE}Iniciando $NUM_CONSUMERS consumers...${NC}"
for ((i=1; i<=NUM_CONSUMERS; i++)); do
    # Iniciar consumer redireccionando salida a /dev/null
    stdbuf -oL -eL ./consumer > consumer_${i}.log 2>&1 &
    CONSUMER_PID=$!
    echo -e "${GREEN}Consumer $i iniciado con PID $CONSUMER_PID${NC}"
    
    # Pequeña pausa para evitar sobrecarga
    #sleep 0.2
done

# Lanzar producers en un bucle
echo -e "${BLUE}Iniciando fase de prueba con $NUM_PRODUCERS producers...${NC}"
echo -e "${YELLOW}La prueba durará aproximadamente $TEST_DURATION segundos${NC}"

#Usar un enfoque más simple para distribuir producers a lo largo del tiempo
DELAY=$((TEST_DURATION / NUM_PRODUCERS))
if [ $DELAY -lt 1 ]; then
    DELAY=1
fi

for ((i=1; i<=NUM_PRODUCERS; i++)); do
    ./producer > /dev/null 2>&1 &
    PRODUCER_PID=$!
    echo -e "${GREEN}Producer $i iniciado con PID $PRODUCER_PID${NC}"
    
    #Esperar un intervalo pequeño para el siguiente producer
    #sleep $DELAY
done

echo -e "${BLUE}Todos los producers han sido lanzados. Esperando a que finalice la prueba...${NC}"

# Esperar un tiempo adicional para asegurar que la prueba completa su ejecución
REMAINING=$((TEST_DURATION / 4))
if [ $REMAINING -gt 0 ]; then
    echo -e "${YELLOW}Esperando $REMAINING segundos adicionales para completar la prueba...${NC}"
    #sleep $REMAINING
fi

# Esperar un poco más para que terminen todas las operaciones
COOLDOWN=5
echo -e "${YELLOW}Esperando $COOLDOWN segundos adicionales para que se completen todas las operaciones...${NC}"
#sleep $COOLDOWN



echo -e "${BLUE}Prueba completada.${NC}"
# echo -e "${BLUE}Terminar con CRTL + C${NC}"
# while true; do
#     # Esperar indefinidamente para mantener el script activo
    
#     sleep 1
# done
# La limpieza se hará automáticamente por la función trap al salir
# La limpieza se hará automáticamente por la función trap al salir



