CC = gcc
CFLAGS = -Wall -Wextra -O2 -pthread

SRC_DIR = src
BIN_DIR = $(SRC_DIR)
TARGETS = broker consumer producer

all: $(TARGETS)

broker: $(SRC_DIR)/broker.c
	$(CC) $(CFLAGS) -o $(BIN_DIR)/broker $(SRC_DIR)/broker.c

consumer: $(SRC_DIR)/consumer.c
	$(CC) $(CFLAGS) -o $(BIN_DIR)/consumer $(SRC_DIR)/consumer.c

producer: $(SRC_DIR)/producer.c
	$(CC) $(CFLAGS) -o $(BIN_DIR)/producer $(SRC_DIR)/producer.c

clean:
	rm -f $(BIN_DIR)/broker $(BIN_DIR)/consumer $(BIN_DIR)/producer *.o $(SRC_DIR)/*.o broker.log mensajes.log consumer_*.log

.PHONY: all clean