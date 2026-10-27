# Arquitectura 1: Procesamiento de Pedidos con RabbitMQ

Esta arquitectura implementa un flujo de mensajería asincrónica para el procesamiento desacoplado de pedidos utilizando RabbitMQ como Message Broker y protocolo AMQP.

## Componentes

1. **orders-api (Productor)**: API REST construida con Express que expone el endpoint `POST /orders`, valida la solicitud, genera un identificador único de orden (`orderId`), asigna un `correlationId` para trazabilidad y publica un mensaje persistente en la cola `pedidos`. Responde de inmediato al cliente (`202 Accepted`) sin esperar al consumidor.
2. **rabbitmq (Broker)**: Contenedor con `rabbitmq:4-management` que expone los puertos 5672 (AMQP) y 15672 (Consola Web de Administración). Mantiene la cola durable `pedidos` y gestiona la entrega de mensajes.
3. **notification-worker (Consumidor)**: Worker independiente que consume mensajes de la cola `pedidos`, simula un retardo de procesamiento de 3 segundos (permitiendo observar mensajes en estado `Unacked`) y confirma manualmente con `ack`. Configurado con `prefetch(1)` para permitir balanceo de carga equitativo.

## Variables de Entorno

| Variable | Servicio | Valor por Defecto | Descripción |
|---|---|---|---|
| `PORT` | `orders-api` | `3000` | Puerto HTTP del servicio REST |
| `RABBITMQ_URL` | Ambos | `amqp://admin:admin@rabbitmq:5672` | URL de conexión AMQP interna |
| `QUEUE_NAME` | Ambos | `pedidos` | Nombre de la cola durable |
| `PROCESSING_DELAY_MS` | `notification-worker` | `3000` | Tiempo de simulación de procesamiento |

## Comandos de Ejecución

### 1. Construir y levantar los servicios
```bash
docker compose up -d --build
```

### 2. Verificar estado de los contenedores
```bash
docker compose ps
```

### 3. Consultar logs en tiempo real
```bash
docker compose logs -f
```

### 4. Probar envío de pedido vía HTTP
```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName": "Carlos", "product": "Libro de arquitectura", "quantity": 1}'
```

### 5. Escalar el consumidor a 2 instancias (Prueba de Elasticidad CP-10)
```bash
docker compose up -d --scale notification-worker=2
```

### 6. Detener el consumidor para pruebas de resiliencia (CP-09 / CP-11)
```bash
docker compose stop notification-worker
```

### 7. Reanudar el consumidor
```bash
docker compose start notification-worker
```

### 8. Detener todos los servicios
```bash
docker compose down
```

## Consola de Administración RabbitMQ
- **URL**: [http://localhost:15672](http://localhost:15672)
- **Usuario**: `admin`
- **Contraseña**: `admin`
- **Métricas observables**: Mensajes en estado `Ready`, `Unacked` y `Total`.
