# Taller: Pruebas de Integración en Arquitecturas Basadas en Mensajes y Eventos

**Institución:** Institución Universitaria ITM  
**Asignatura:** Aseguramiento de la Calidad del Software (ACS) — Semestre 8  
**Ponderación:** 20%  

---

## 1. Descripción del Proyecto

Este repositorio contiene la solución completa, contenerizada y verificada para el taller universitario de **Pruebas de Integración en Arquitecturas Basadas en Mensajes y Eventos**, siguiendo estrictamente la guía y rúbrica oficial:

1. **Arquitectura 1 (RabbitMQ - AMQP):** Procesamiento asincrónico de pedidos mediante cola de trabajo (`Work Queue`), con API REST productora (`orders-api`), broker RabbitMQ 4 con interfaz de gestión web, y worker consumidor (`notification-worker`) con retardo simulado, confirmación manual (`ack`) y balanceo por `prefetch(1)`.
2. **Arquitectura 2 (Apache Kafka - Event-Driven):** Procesamiento reactivo basado en un log distribuido persistente, con API REST productora (`orders-api`) que emite eventos de dominio `OrderCreated` en el tópico `orders.events` (particionado), broker Kafka en modo KRaft, interfaz gráfica Kafka UI y tres consumidores funcionales independientes (`inventory-consumer`, `billing-consumer`, `notification-consumer`) en distintos *Consumer Groups* (patrón *Fan-out*).
3. **Automatización de Pruebas:** Suite automatizada en Node.js que evalúa los 12 casos de prueba de la guía (CP-01 a CP-12) para un total de 20 ejecuciones formales.
4. **Informe Técnico en PDF:** Documento universitario completo listo para revisión en `informe/informe-pruebas-integracion.pdf`.

---

## 2. Estructura del Repositorio

```
Pruebas de integración Mensajes y eventos/
├── rabbitmq/                      # ARQUITECTURA 1: RABBITMQ
│   ├── orders-api/                # API REST Productora (Express + amqplib)
│   │   ├── src/server.js
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── notification-worker/       # Consumidor con prefetch(1) y manual ack
│   │   ├── src/worker.js
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── docker-compose.yml         # Orquestación contenerizada RabbitMQ
│   └── README.md                  # Documentación específica RabbitMQ
│
├── kafka/                         # ARQUITECTURA 2: APACHE KAFKA
│   ├── orders-api/                # API REST Productora de eventos OrderCreated
│   │   ├── src/server.js
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── inventory-consumer/        # Consumidor de inventario (inventory-service-group)
│   │   ├── src/consumer.js
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── billing-consumer/          # Consumidor de facturación (billing-service-group)
│   │   ├── src/consumer.js
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── notification-consumer/     # Consumidor de notificaciones (notification-service-group)
│   │   ├── src/consumer.js
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── docker-compose.yml         # Orquestación Kafka KRaft + Kafka UI + Consumidores
│   └── README.md                  # Documentación específica Kafka
│
├── pruebas/                       # AUTOMATIZACIÓN DE PRUEBAS DE INTEGRACIÓN
│   ├── scripts/
│   │   ├── test-rabbitmq.js       # Suite de pruebas para RabbitMQ (10 casos)
│   │   ├── test-kafka.js          # Suite de pruebas para Apache Kafka (10 casos)
│   │   └── run-all-tests.js       # Ejecutor maestro consolidado (20 casos)
│   ├── resultados/
│   │   ├── resultados-ejecucion.json # Reporte JSON con metadata y métricas
│   │   └── matriz-evidencias.md      # Matriz resumen en Markdown
│   └── package.json
│
├── evidencias/                    # EVIDENCIAS REALES Y TRAZABILIDAD
│   ├── rabbitmq/                  # Diagrama SVG, logs y respuestas HTTP de RabbitMQ
│   └── kafka/                     # Diagrama SVG, logs y respuestas HTTP de Kafka
│
├── informe/                       # INFORME TÉCNICO UNIVERSITARIO
│   ├── informe-pruebas-integracion.md   # Informe técnico en Markdown
│   ├── informe-pruebas-integracion.html # Plantilla renderizada con estilos
│   ├── informe-pruebas-integracion.pdf  # INFORME FINAL LISTO PARA ENTREGA (PDF)
│   └── generate-pdf.js                  # Compilador a PDF con Microsoft Edge
│
└── README.md                      # Esta guía maestra de reproducción
```

---

## 3. Requisitos Previos

- **Node.js:** v18 o superior (verificado con v24.19.0).
- **npm:** v9 o superior (verificado con v11.17.0).
- **Docker Desktop & Docker Compose:** Necesario para ejecutar los contenedores.
  - *Nota:* Si Docker Desktop no está instalado en tu máquina, el instalador oficial ya fue descargado en tu Escritorio: `C:\Users\elmam\OneDrive\Desktop\Instalador_Docker_Desktop.exe`. Ejecútalo como Administrador y reinicia si el asistente lo solicita.

---

## 4. Instrucciones de Ejecución Paso a Paso

### 4.1 Arquitectura 1: RabbitMQ

#### 1. Construir y levantar servicios:
```bash
cd rabbitmq
docker compose up -d --build
```

#### 2. Verificar disponibilidad de contenedores y puertos:
```bash
docker compose ps
```
Debe mostrar los siguientes contenedores activos:
- `rabbitmq`: puertos `5672` (AMQP) y `15672` (Web Management).
- `orders-api-rmq`: puerto `3000` (HTTP).
- `notification-worker-1`: consumidor en background.

#### 3. Probar envío de pedido vía HTTP (Postman o curl):
```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName": "Carlos Gómez", "product": "Libro de arquitectura", "quantity": 1}'
```
Respuesta esperada inmediata (`202 Accepted`):
```json
{
  "status": "accepted",
  "message": "Pedido recibido y enviado a RabbitMQ",
  "orderId": "ORD-...",
  "correlationId": "corr-..."
}
```

#### 4. Observar logs y trazabilidad:
```bash
docker compose logs -f
```

#### 5. Escalar consumidores (Prueba de Elasticidad CP-10):
```bash
docker compose up -d --scale notification-worker=2
```

#### 6. Simular caída de consumidor (Prueba Resilient CP-09 y Desacoplamiento CP-11):
```bash
docker compose stop notification-worker
# Enviar pedidos... los mensajes quedarán retenidos en estado 'Ready' en RabbitMQ
docker compose start notification-worker
# El worker procesará los mensajes acumulados sin pérdidas
```

#### 7. Acceso a la Consola RabbitMQ Management:
- **URL:** [http://localhost:15672](http://localhost:15672)
- **Usuario:** `admin` | **Contraseña:** `admin`

---

### 4.2 Arquitectura 2: Apache Kafka

#### 1. Construir y levantar servicios:
```bash
cd kafka
docker compose up -d --build
```

#### 2. Verificar disponibilidad:
```bash
docker compose ps
```
Debe mostrar los siguientes contenedores activos:
- `kafka`: puertos `29092` (externo) y `9092` (interno).
- `kafka-ui`: puerto `8080`.
- `orders-api-kafka`: puerto `3000`.
- `inventory-consumer`: consumidor de inventario (`inventory-service-group`).
- `billing-consumer`: consumidor de facturación (`billing-service-group`).
- `notification-consumer`: consumidor de notificaciones (`notification-service-group`).

#### 3. Probar publicación de evento vía HTTP:
```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName": "Carlos Gómez", "product": "Libro de arquitectura", "quantity": 1, "unitPrice": 85000}'
```
Respuesta esperada (`202 Accepted`):
```json
{
  "status": "accepted",
  "message": "Pedido recibido y evento OrderCreated publicado en Kafka",
  "orderId": "ORD-...",
  "eventId": "evt-...",
  "correlationId": "corr-...",
  "topic": "orders.events",
  "partition": 0,
  "offset": "..."
}
```

#### 4. Observar recepción en los 3 consumidores (Fan-out):
```bash
docker compose logs -f
```
Verás cómo los tres consumidores procesan concurrentemente el mismo evento `OrderCreated`.

#### 5. Escalar consumidor para balanceo de particiones (CP-10 Elastic):
```bash
docker compose up -d --scale inventory-consumer=2
```

#### 6. Simular caída y recuperación (CP-09, CP-11 y CP-12):
```bash
docker compose stop inventory-consumer
# Publicar eventos... Facturación y Notificación continúan procesando normalmente
docker compose start inventory-consumer
# Inventario lee desde su último offset o desde el inicio (fromBeginning: true)
```

#### 7. Acceso a la Consola Kafka UI:
- **URL:** [http://localhost:8080](http://localhost:8080)
- Inspecciona el tópico `orders.events`, mensajes en Partición 0 y 1, y los tres consumer groups.

---

## 5. Ejecución Automatizada de Pruebas

Para ejecutar la suite de pruebas completa:

```bash
node pruebas/scripts/run-all-tests.js
```

O por arquitectura:
```bash
# Pruebas de RabbitMQ (10 casos)
node pruebas/scripts/test-rabbitmq.js

# Pruebas de Apache Kafka (10 casos)
node pruebas/scripts/test-kafka.js
```

Los resultados consolidados se guardan automáticamente en:
- `pruebas/resultados/resultados-ejecucion.json`
- `pruebas/resultados/matriz-evidencias.md`

---

## 6. Generación del Informe Técnico en PDF

El informe técnico final ya está compilado y disponible en:
`informe/informe-pruebas-integracion.pdf`

Si deseas regenerarlo tras realizar modificaciones:
```bash
node informe/generate-pdf.js
```
El script utiliza el motor headless de Microsoft Edge para compilar el archivo HTML con diagramas SVG vectoriales, tablas y portada formal a un PDF universitario de alta fidelidad.

---

## 7. Matriz Resumen de Casos de Prueba

| ID | Caso de Prueba | RabbitMQ | Apache Kafka | Estado |
|---|---|---|---|---|
| **CP-01** | Creación de pedido vía API | Aplicable | Aplicable | ✅ Cumple |
| **CP-02** | Envío de mensaje a RabbitMQ | Aplicable | N/A | ✅ Cumple |
| **CP-03** | Consumo de mensaje (RabbitMQ) | Aplicable | N/A | ✅ Cumple |
| **CP-04** | Publicación de evento en Kafka | N/A | Aplicable | ✅ Cumple |
| **CP-05** | Consumo de evento (Kafka) | N/A | Aplicable | ✅ Cumple |
| **CP-06** | Trazabilidad del flujo | Aplicable | Aplicable | ✅ Cumple |
| **CP-07** | Responsive | Aplicable | Aplicable | ✅ Cumple |
| **CP-08** | Message-driven | Aplicable | Aplicable | ✅ Cumple |
| **CP-09** | Resilient | Aplicable | Aplicable | ✅ Cumple |
| **CP-10** | Elastic | Aplicable | Aplicable | ✅ Cumple |
| **CP-11** | Desacoplamiento temporal | Aplicable | Aplicable | ✅ Cumple |
| **CP-12** | Recuperación | Aplicable | Aplicable | ✅ Cumple |

Total: **20 reportes de ejecución individuales** incluidos en el informe técnico.
