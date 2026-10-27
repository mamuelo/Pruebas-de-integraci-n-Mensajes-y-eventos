# Arquitectura 2: Procesamiento Orientado a Eventos con Apache Kafka

Esta arquitectura implementa un sistema reactivo y orientado a eventos (EDA - Event-Driven Architecture) para el procesamiento distribuido de pedidos mediante Apache Kafka y visualización a través de Kafka UI.

## Componentes

1. **orders-api (Productor)**: API REST con Express que expone `POST /orders`, valida datos, calcula el total del pedido (`quantity * unitPrice`), genera un evento de dominio estricto `OrderCreated` con clave de partición (`orderId`) y metadatos (`eventId`, `eventType`, `eventVersion`, `occurredAt`, `source`, `correlationId`, `data`). Publica en el tópico `orders.events` y responde inmediatamente con `202 Accepted`.
2. **kafka (Broker / Log Persistente)**: Broker Apache Kafka en modo KRaft (sin ZooKeeper) configurado con el tópico `orders.events` particionado (mínimo 2 particiones) y almacenamiento persistente en volumen.
3. **kafka-ui (Consola de Observación)**: Interfaz gráfica web en puerto 8080 para monitoreo de tópicos, particiones, mensajes, offsets y consumer groups.
4. **Consumidores de Dominio Independientes**:
   - `inventory-consumer` (`inventory-service-group`): Consume `OrderCreated` y simula la reserva o descuento de inventario.
   - `billing-consumer` (`billing-service-group`): Consume `OrderCreated` y simula la emisión de la factura y registro contable.
   - `notification-consumer` (`notification-service-group`): Consume `OrderCreated` y simula el despacho de correos/mensajes al cliente.

Cada consumidor pertenece a un grupo funcional distinto, lo que garantiza que los tres reciben cada evento de forma independiente (Fan-out / Publish-Subscribe).

## Variables de Entorno

| Variable | Servicio | Valor por Defecto | Descripción |
|---|---|---|---|
| `PORT` | `orders-api` | `3000` | Puerto HTTP del servicio REST |
| `KAFKA_BROKER` | Todos | `kafka:9092` | Dirección del broker dentro de Docker |
| `KAFKA_TOPIC` | Todos | `orders.events` | Nombre del tópico de eventos |
| `NUM_PARTITIONS` | `orders-api` | `2` | Particiones mínimas requeridas |
| `KAFKA_GROUP_ID` | Consumidores | Específico por servicio | Identificador del consumer group |

## Comandos de Ejecución

### 1. Construir y levantar los servicios
```bash
docker compose up -d --build
```

### 2. Verificar estado de los contenedores
```bash
docker compose ps
```

### 3. Visualizar logs en tiempo real
```bash
docker compose logs -f
```

### 4. Probar publicación de evento vía HTTP
```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName": "Pedro", "product": "Libro de arquitectura", "quantity": 1, "unitPrice": 85000}'
```

### 5. Escalar consumidor para balanceo de carga (Prueba de Elasticidad CP-10)
```bash
docker compose up -d --scale inventory-consumer=2
```

### 6. Detener un consumidor para pruebas de resiliencia y desacoplamiento temporal (CP-09 / CP-11)
```bash
docker compose stop inventory-consumer
```

### 7. Iniciar nuevamente el consumidor
```bash
docker compose start inventory-consumer
```

### 8. Detener todos los servicios
```bash
docker compose down
```

## Consola de Observabilidad Kafka UI
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Métricas observables**: Tópico `orders.events`, mensajes publicados, particiones 0 y 1, grupos de consumidores (`inventory-service-group`, `billing-service-group`, `notification-service-group`) y sus respectivos offsets de lectura.
