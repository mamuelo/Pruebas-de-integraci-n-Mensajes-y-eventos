# INFORME TÉCNICO DE PRUEBAS DE INTEGRACIÓN EN ARQUITECTURAS BASADAS EN MENSAJES Y EVENTOS

---

## 1. Portada

**Institución:** Institución Universitaria ITM  
**Facultad:** Facultad de Ingenierías  
**Programa Académico:** Ingeniería de Sistemas  
**Asignatura:** Aseguramiento de la Calidad del Software (ACS)   
**Taller:** Pruebas de Integración en Arquitecturas Basadas en Mensajes y Eventos (Ponderación 20%)  
**Docente:** Carlos Andrés Jaramillo Patiño

**Integrantes:**
- Manuela Sánchez Pareja
- Emanuel Ortiz Areiza 
- Juan Felipe Torres


**Fecha de Entrega:** 1 de noviembre de 2026  
**Medellín, Colombia**

---

## 2. Introducción

En los sistemas distribuidos modernos, la comunicación síncrona mediante llamadas HTTP tradicionales introduce un alto acoplamiento temporal y espacial, cuellos de botella ante sobrecargas y fragilidad ante la caída de servicios dependientes. Para superar estas restricciones, las arquitecturas orientadas a mensajes (Message-Driven) y guiadas por eventos (Event-Driven) establecen un canal asincrónico mediado por intermediarios especializados (*brokers*) que desacoplan los productores de los consumidores.

Desde la perspectiva del Aseguramiento de la Calidad del Software (QA), este paradigma exige transformar radicalmente la estrategia de pruebas. La verificación ya no puede limitarse a validar códigos de estado y cargas útiles en la capa HTTP. Es imperativo evaluar el comportamiento en el broker, la persistencia de mensajes y registros, el reparto equitativo de carga, la tolerancia a caídas temporales, el desacoplamiento temporal y el cumplimiento riguroso de las propiedades reactivas del sistema (Responsive, Resilient, Elastic y Message-Driven).

Este informe técnico documenta el diseño, la implementación contenerizada, la automatización y la ejecución de las pruebas de integración sobre dos arquitecturas reactivas independientes: una basada en colas de mensajes con **RabbitMQ** y otra basada en un log persistente de eventos con **Apache Kafka**.

---

## 3. Objetivo General y Alcance

### 3.1 Objetivo General
Diseñar, implementar, ejecutar y documentar pruebas de integración sobre dos arquitecturas reactivas independientes (RabbitMQ y Apache Kafka), demostrando mediante evidencia técnica empírica y verificable el correcto funcionamiento del sistema, el flujo asincrónico y el cumplimiento de las propiedades reactivas.

### 3.2 Alcance
- **Enfoque exclusivo:** Pruebas de integración entre componentes (API REST productora, Broker de mensajería y Consumidores/Workers independientes).
- **Validaciones:**
  - Comunicación asincrónica desacoplada.
  - Verificación del flujo de vida del mensaje/evento entre productor, broker y consumidor.
  - Trazabilidad de pedidos mediante `orderId` y `correlationId`.
  - Evaluación rigurosa de las propiedades reactivas:
    - *Responsive:* respuesta inmediata de la API sin bloqueo.
    - *Message-driven / Event-driven:* aislamiento total sin invocaciones HTTP directas entre servicios.
    - *Resilient:* tolerancia a fallos y caída de consumidores sin pérdida de datos.
    - *Elastic:* distribución balanceada de carga entre múltiples instancias concurrentes.
    - *Desacoplamiento temporal:* capacidad de encolar o almacenar mensajes mientras los consumidores están inactivos y procesarlos al reactivarse.
    - *Recuperación:* reprocesamiento íntegro sin pérdidas ante reinicios del consumidor o del broker.
- **Fuera de alcance:** Lógica de negocio compleja de dominio y pruebas unitarias aisladas de funciones internas.

---

## 4. Descripción de las Arquitecturas y Diagramas

### 4.1 Arquitectura 1: Procesamiento de Pedidos con RabbitMQ

Esta arquitectura sigue el patrón de cola de trabajo (*Work Queue / Task Distribution*) mediado por el protocolo AMQP (Advanced Message Queuing Protocol).

#### Componentes:
1. **orders-api (Productor):** Servicio web REST desarrollado en Node.js y Express. Expone el endpoint `POST /orders`, valida la integridad de los datos (`customerName`, `product`, `quantity`), genera un `orderId` único y un `correlationId` para trazabilidad global, publica el mensaje en la cola durable `pedidos` con la bandera `persistent: true` y responde de forma inmediata (`202 Accepted`) al cliente HTTP sin esperar al consumidor.
2. **rabbitmq (Broker):** Instancia oficial `rabbitmq:4-management`. Expone el puerto AMQP `5672` para comunicación interna entre contenedores y el puerto `15672` para la consola web de gestión. Administra la cola `pedidos`, manteniendo contadores en tiempo real de mensajes `Ready`, `Unacked` y `Total`.
3. **notification-worker (Consumidor):** Servicio consumidor independiente conectado mediante AMQP. Escucha la cola `pedidos`, simula un retardo de procesamiento de 3000 ms (para facilitar la observación de mensajes en vuelo `Unacked`), procesa el pedido y confirma manualmente la entrega mediante `channel.ack(msg)`. Configurado con `channel.prefetch(1)` para garantizar balanceo equitativo entre múltiples instancias. No expone ningún puerto HTTP.

```
+---------------------------------------------------------------------------------------+
| DOCKER HOST (Bridge Network: rabbitmq-net)                                            |
|                                                                                       |
|  [Cliente HTTP]                                                                       |
|   (Postman/curl)                                                                      |
|        |                                                                              |
|        | POST /orders (HTTP)                                                          |
|        v                                                                              |
|  +--------------------+   AMQP publish (persistent)   +----------------------------+  |
|  | orders-api         | ----------------------------> | rabbitmq:4-management     |  |
|  | (Puerto 3000)      |                               | - AMQP: 5672               |  |
|  | - Valida payload   |                               | - Web Console: 15672       |  |
|  | - Resp. Inmediata  |                               | - Cola: 'pedidos'          |  |
|  +--------------------+                               +----------------------------+  |
|                                                              |           |            |
|                                                  AMQP consume|           |AMQP consume|
|                                                  prefetch(1) |           |prefetch(1) |
|                                                              v           v            |
|                                                    +-------------+ +---------------+  |
|                                                    |notification-| | notification- |  |
|                                                    |worker (Inst)| | worker (Inst2)|  |
|                                                    | (Manual ACK)| | (Manual ACK)  |  |
|                                                    +-------------+ +---------------+  |
+---------------------------------------------------------------------------------------+
```

---

### 4.2 Arquitectura 2: Procesamiento Orientado a Eventos con Apache Kafka

Esta arquitectura implementa un modelo de eventos distribuidos de publicación/suscripción (*Event Log / Pub-Sub*) con persistencia inmutable en disco y consumo independiente por grupos funcionales.

#### Componentes:
1. **orders-api (Productor):** API REST en Node.js y Express. Expone `POST /orders`, valida datos (`customerName`, `product`, `quantity`, `unitPrice`), calcula el total financiero (`quantity * unitPrice`), genera un evento de dominio estricto `OrderCreated` con clave de partición `orderId` y metadatos de contrato (`eventId`, `eventType`, `eventVersion`, `occurredAt`, `source`, `correlationId`, `data`). Publica el evento en el tópico `orders.events` y responde inmediatamente al cliente (`202 Accepted`).
2. **kafka (Broker / Event Log):** Broker Apache Kafka en modo KRaft (sin ZooKeeper) que expone el puerto interno `9092` y el puerto externo `29092`. Configurado con el tópico `orders.events` distribuido en al menos 2 particiones para balanceo de carga.
3. **kafka-ui (Consola de Observación):** Interfaz gráfica web expuesta en el puerto `8080` conectada al cluster local para auditar tópicos, mensajes, particiones, offsets y consumer groups.
4. **Consumidores Funcionales Independientes:**
   - `inventory-consumer`: Suscrito al tópico bajo el grupo `inventory-service-group`. Simula la reserva o descuento físico de existencias.
   - `billing-consumer`: Suscrito al tópico bajo el grupo `billing-service-group`. Simula la facturación y el cobro del total calculado.
   - `notification-consumer`: Suscrito al tópico bajo el grupo `notification-service-group`. Simula el despacho de correos electrónicos y SMS.

Cada consumidor utiliza su propio grupo de consumo, garantizando el patrón *Fan-out* (los tres servicios reciben y procesan cada evento publicado).

```
+-----------------------------------------------------------------------------------------------+
| DOCKER HOST (Bridge Network: kafka-net)                                                       |
|                                                                                               |
|  [Cliente HTTP]                                                                               |
|   (Postman/curl)                                                                              |
|        |                                                                                      |
|        | POST /orders (HTTP)                                                                  |
|        v                                                                                      |
|  +--------------------+      TCP:9092 (Event)         +------------------------------------+  |
|  | orders-api (Kafka) | ----------------------------> | kafka (KRaft Broker)               |  |
|  | (Puerto 3000)      |                               | - Tópico: 'orders.events'          |  |
|  | - Event:           |                               |   * Partición 0 (Offsets: 0,1,..)  |  |
|  |   OrderCreated     |                               |   * Partición 1 (Offsets: 0,1,..)  |  |
|  | - Resp. Inmediata  |                               | - Puerto externo: 29092            |  |
|  +--------------------+                               +------------------------------------+  |
|                                                              |     |     |     |              |
|                                                 +------------+     |     |     +-----------+  |
|                                                 |                  |     |                 |  |
|                                                 v                  v     v                 v  |
|                                           +------------+ +------------+ +------------+ +-----+  |
|                                           | inventory- | |  billing-  | |notification| |Kafka|  |
|                                           |  consumer  | |  consumer  | |  consumer  | | UI  |  |
|                                           | (G: invent)| | (G: bill)  | | (G: notif) | |:8080|  |
|                                           +------------+ +------------+ +------------+ +-----+  |
+-----------------------------------------------------------------------------------------------+
```

---

## 5. Tecnologías y Configuración del Entorno

| Componente | Tecnología Seleccionada | Versión / Imagen | Rol / Propósito |
|---|---|---|---|
| **Runtime Base** | Node.js / V8 | Node.js v24.19.0 / npm 11.17.0 | Ejecución de microservicios y suites de pruebas |
| **Framework HTTP** | Express.js | 4.21.2 | Exposición de endpoints REST asincrónicos |
| **Cliente AMQP** | amqplib | 0.10.4 | Protocolo binario AMQP 0-9-1 para RabbitMQ |
| **Cliente Kafka** | kafkajs | 2.2.4 | Driver nativo de Apache Kafka para Node.js |
| **Broker Mensajería** | RabbitMQ | `rabbitmq:4-management` | Message broker con cola durable y UI web |
| **Broker Eventos** | Apache Kafka | `bitnamilegacy/kafka:3.7` | Log de eventos distribuido en modo KRaft |
| **Observabilidad Kafka** | Kafka UI | `provectuslabs/kafka-ui:latest` | Consola web gráfica de inspección de eventos |
| **Orquestador** | Docker & Docker Compose | Docker Desktop v4.97 / Compose v2 | Contenerización y redes aisladas bridge |
| **Renderizador PDF** | Microsoft Edge Headless | Edge Chromium CLI | Generación de informe PDF de alta fidelidad |

---

## 6. Procedimiento de Despliegue

### 6.1 Despliegue de la Arquitectura RabbitMQ
1. Navegar al directorio de RabbitMQ:
   ```bash
   cd rabbitmq
   ```
2. Construir imágenes e iniciar los contenedores en segundo plano:
   ```bash
   docker compose up -d --build
   ```
3. Verificar el estado de salud de los servicios:
   ```bash
   docker compose ps
   ```
4. Consultar los registros de conexión:
   ```bash
   docker compose logs -f
   ```

### 6.2 Despliegue de la Arquitectura Apache Kafka
1. Navegar al directorio de Kafka:
   ```bash
   cd kafka
   ```
2. Levantar el broker Kafka, Kafka UI, orders-api y los 3 consumidores funcionales:
   ```bash
   docker compose up -d --build
   ```
3. Comprobar que los 6 contenedores estén activos:
   ```bash
   docker compose ps
   ```
4. Verificar la creación automática del tópico con 2 particiones en Kafka UI (`http://localhost:8080`).

---

## 7. Verificación de Contenedores, Puertos y Consolas

### 7.1 Puertos y Consolas de Acceso

| Arquitectura | Servicio | Puerto Host | Protocolo | Interfaz / Consola |
|---|---|---|---|---|
| **RabbitMQ** | `orders-api` | `3000` | HTTP | API REST (`POST /orders`, `GET /health`) |
| **RabbitMQ** | `rabbitmq` | `5672` | AMQP | Comunicación TCP entre microservicios |
| **RabbitMQ** | `rabbitmq` | `15672` | HTTP | Consola Web RabbitMQ Management (`admin:admin`) |
| **Kafka** | `orders-api` | `3000` | HTTP | API REST (`POST /orders`, `GET /health`) |
| **Kafka** | `kafka` | `29092` | PLAINTEXT | Listener externo para herramientas locales |
| **Kafka** | `kafka` (interno) | `9092` | PLAINTEXT | Comunicación interna Docker |
| **Kafka** | `kafka-ui` | `8080` | HTTP | Consola Web Kafka UI |

---

## 8. Reportes Individuales de Ejecución de Pruebas

A continuación se presentan los veinte (20) reportes individuales organizados por arquitectura, cubriendo los casos funcionales (CP-01 a CP-06) y reactivos (CP-07 a CP-12).

---

### Bloque A: Reportes de Ejecución — Arquitectura 1: RabbitMQ

#### Reporte RMQ-01: Creación de pedido vía API (CP-01)
- **ID del Caso:** CP-01
- **Arquitectura:** RabbitMQ
- **Objetivo:** Validar que el endpoint `POST /orders` recibe el pedido, valida la entrada y responde con estado `accepted` y un `orderId` generado.
- **Precondiciones:** Servicio `orders-api` en ejecución conectado a RabbitMQ.
- **Datos de Entrada:** `{"customerName": "Carlos Gómez", "product": "Libro de arquitectura", "quantity": 1}`.
- **Pasos Ejecutados:**
  1. Enviar solicitud POST mediante cliente HTTP a `http://localhost:3000/orders`.
  2. Medir código de estado HTTP y tiempo transcurrido.
  3. Verificar presencia de campos obligatorios en el JSON de respuesta.
- **Resultado Esperado:** Código HTTP 202 Accepted, cuerpo con `status: "accepted"`, `orderId` y `correlationId`.
- **Resultado Obtenido:** HTTP 202 Accepted en 14 ms. Payload: `{"status":"accepted","message":"Pedido recibido y enviado a RabbitMQ","orderId":"ORD-1729992015000-4821","correlationId":"corr-48f864be-86eb-42f0-9366-0d676839a8e2"}`.
- **Evidencia:** Archivo `evidencias/rabbitmq/rabbitmq-http-post-response.json`.
- **Estado:** Cumple.
- **Análisis Técnico:** La API valida adecuadamente las restricciones del negocio y responde en un tiempo ultra-bajo sin bloquear el hilo de ejecución principal.

---

#### Reporte RMQ-02: Envío de mensaje a RabbitMQ (CP-02)
- **ID del Caso:** CP-02
- **Arquitectura:** RabbitMQ
- **Objetivo:** Verificar que el pedido recibido genera un mensaje persistente en la cola `pedidos`.
- **Precondiciones:** Broker RabbitMQ activo, cola `pedidos` declarada con `durable: true`.
- **Datos de Entrada:** Invocación del endpoint `POST /orders` con datos válidos.
- **Pasos Ejecutados:**
  1. Conectar al canal AMQP del broker.
  2. Verificar estado inicial de la cola `pedidos`.
  3. Emitir solicitud de pedido a la API.
  4. Constatar incremento del mensaje y persistencia de cabeceras en el broker.
- **Resultado Esperado:** Mensaje publicado en cola `pedidos` con entrega persistente (`persistent: true`).
- **Resultado Obtenido:** Mensaje encolado exitosamente con ID `ORD-1729992015000-4821` y `deliveryMode: 2` (persistente).
- **Evidencia:** Archivo `evidencias/rabbitmq/rabbitmq-orders-api-logs.txt`.
- **Estado:** Cumple.
- **Análisis Técnico:** El mensaje es almacenado en disco por el motor Mnesia de RabbitMQ, garantizando su supervivencia en caso de caída imprevista del broker.

---

#### Reporte RMQ-03: Consumo de mensaje en RabbitMQ (CP-03)
- **ID del Caso:** CP-03
- **Arquitectura:** RabbitMQ
- **Objetivo:** Validar que el worker `notification-worker` consume el mensaje, simula el procesamiento y emite la confirmación manual (ACK).
- **Precondiciones:** `notification-worker` activo suscrito a la cola `pedidos` con `noAck: false`.
- **Datos de Entrada:** Mensaje con `orderId=ORD-1729992015000-4821`.
- **Pasos Ejecutados:**
  1. Publicar pedido en la cola.
  2. Observar transición del mensaje a estado `Unacked` durante el retardo de 3000 ms.
  3. Verificar emisión de log de confirmación y remoción del mensaje de la cola.
- **Resultado Esperado:** Procesamiento simulado de 3 segundos, generación de log y envío de ACK explícito.
- **Resultado Obtenido:** Worker consumió mensaje a las `20:45:10.135Z`, simuló 3000 ms de despacho y ejecutó `channel.ack(msg)` a las `20:45:13.140Z`.
- **Evidencia:** Archivo `evidencias/rabbitmq/rabbitmq-worker-logs.txt`.
- **Estado:** Cumple.
- **Análisis Técnico:** La confirmación manual impide la pérdida de mensajes durante la ejecución de tareas asincrónicas lentas.

---

#### Reporte RMQ-06: Trazabilidad del flujo (CP-06)
- **ID del Caso:** CP-06
- **Arquitectura:** RabbitMQ
- **Objetivo:** Seguir el mismo pedido a través de su `orderId` y `correlationId` en la API, el broker y el consumidor.
- **Precondiciones:** Pipeline completo en ejecución.
- **Datos de Entrada:** Pedido con `orderId=ORD-1729992015000-4821` y `correlationId=corr-48f864be-86eb-42f0-9366-0d676839a8e2`.
- **Pasos Ejecutados:**
  1. Capturar identificadores retornados en la respuesta HTTP.
  2. Localizar dichos identificadores en los logs de publicación de `orders-api`.
  3. Localizar los mismos identificadores en los registros de recepción y finalización de `notification-worker`.
- **Resultado Esperado:** Presencia idéntica e inmutable del `correlationId` en todos los puntos de traza.
- **Resultado Obtenido:** Coincidencia exacta del `correlationId` y `orderId` en ambos servicios sin degradación de metadatos.
- **Evidencia:** Comparación de trazas en `rabbitmq-orders-api-logs.txt` y `rabbitmq-worker-logs.txt`.
- **Estado:** Cumple.
- **Análisis Técnico:** El patrón Correlation Identifier implementado asegura observabilidad completa en entornos desacoplados.

---

#### Reporte RMQ-07: Responsive: respuesta sin esperar al consumidor (CP-07)
- **ID del Caso:** CP-07
- **Arquitectura:** RabbitMQ
- **Objetivo:** Demostrar que la API responde inmediatamente al cliente sin bloquearse por la duración del consumidor (~3000 ms).
- **Precondiciones:** `notification-worker` configurado con retardo de 3000 ms.
- **Datos de Entrada:** Solicitud POST a `/orders`.
- **Pasos Ejecutados:**
  1. Registrar timestamp inicial del request HTTP.
  2. Registrar timestamp de recepción de respuesta HTTP.
  3. Comparar latencia HTTP (14 ms) frente a la duración de ejecución del worker (3005 ms).
- **Resultado Esperado:** Latencia HTTP << 3000 ms (típicamente < 100 ms).
- **Resultado Obtenido:** Tiempo de respuesta HTTP medido: 14 ms. El worker completó el procesamiento 3000 ms después en segundo plano.
- **Evidencia:** Mediciones de latencia en `rabbitmq-http-post-response.json` vs `rabbitmq-worker-logs.txt`.
- **Estado:** Cumple.
- **Análisis Técnico:** Satisface plenamente el pilar Responsive del Manifiesto Reactivo al desligar el hilo web del procesamiento intensivo.

---

#### Reporte RMQ-08: Message-driven: comunicación mediante broker (CP-08)
- **ID del Caso:** CP-08
- **Arquitectura:** RabbitMQ
- **Objetivo:** Verificar que no existe comunicación directa HTTP ni acoplamiento físico entre la API y el consumidor.
- **Precondiciones:** Inspección de arquitectura y manifiestos Docker Compose.
- **Datos de Entrada:** Configuración de servicios y análisis de código fuente.
- **Pasos Ejecutados:**
  1. Inspeccionar `notification-worker`: constatar ausencia de servidor web (Express/HTTP) y puertos expuestos.
  2. Inspeccionar `orders-api`: constatar que solo establece conexión TCP/AMQP hacia `rabbitmq:5672`.
- **Resultado Esperado:** Aislamiento total entre servicios; transporte 100% mediado por RabbitMQ.
- **Resultado Obtenido:** `notification-worker` carece de puertos expuestos y no expone ninguna interfaz de red entrante. Toda interacción ocurre vía colas AMQP.
- **Evidencia:** Archivos `rabbitmq/docker-compose.yml` y `rabbitmq/notification-worker/src/worker.js`.
- **Estado:** Cumple.
- **Análisis Técnico:** Implementación canónica del principio Message-Driven: emisor y receptor solo conocen el broker y el contrato del mensaje.

---

#### Reporte RMQ-09: Resilient: tolerancia a caída del consumidor (CP-09)
- **ID del Caso:** CP-09
- **Arquitectura:** RabbitMQ
- **Objetivo:** Verificar que la detención del consumidor no degrada la API ni causa pérdida de mensajes.
- **Precondiciones:** `orders-api` y `rabbitmq` activos; `notification-worker` detenido.
- **Datos de Entrada:** Publicación de pedidos con el worker apagado.
- **Pasos Ejecutados:**
  1. Detener el contenedor del consumidor: `docker compose stop notification-worker`.
  2. Enviar 2 pedidos sucesivos a la API.
  3. Verificar respuesta HTTP 202 exitosa.
  4. Consultar la consola de RabbitMQ y verificar que los mensajes quedan retenidos en estado `Ready`.
- **Resultado Esperado:** La API opera sin errores y la cola almacena los mensajes de forma segura.
- **Resultado Obtenido:** La API respondió HTTP 202 con latencia normal. La cola `pedidos` retuvo los 2 mensajes en estado `Ready`.
- **Evidencia:** Métricas de cola `pedidos` en RabbitMQ Management (`Ready=2`).
- **Estado:** Cumple.
- **Análisis Técnico:** La cola actúa como búfer elástico y amortiguador de fallas, aislando al cliente de indisponibilidades en la capa de procesamiento.

---

#### Reporte RMQ-10: Elastic: distribución de carga (CP-10)
- **ID del Caso:** CP-10
- **Arquitectura:** RabbitMQ
- **Objetivo:** Demostrar el reparto balanceado de carga entre múltiples instancias de `notification-worker` mediante `prefetch(1)`.
- **Precondiciones:** Escalar el worker a 2 réplicas: `docker compose up -d --scale notification-worker=2`.
- **Datos de Entrada:** Envío de ráfaga de 4 pedidos consecutivos.
- **Pasos Ejecutados:**
  1. Levantar segunda instancia del worker sin colisión de nombres.
  2. Inyectar 4 pedidos en rápida sucesión.
  3. Inspeccionar logs de ambas instancias para verificar la asignación de pedidos.
- **Resultado Esperado:** Distribución equitativa (50% de pedidos por instancia: 2 y 2) bajo esquema Round-Robin justo.
- **Resultado Obtenido:** `notification-worker-1` procesó los pedidos 1 y 3; `notification-worker-2` procesó los pedidos 2 y 4 de forma intercalada.
- **Evidencia:** Identificadores de host en logs cruzados de las instancias 1 y 2.
- **Estado:** Cumple.
- **Análisis Técnico:** La directiva `channel.prefetch(1)` garantiza que RabbitMQ no asigne un nuevo mensaje a un worker hasta que libere el anterior con ACK, evitando sobrecargas en nodos lentos.

---

#### Reporte RMQ-11: Desacoplamiento temporal (CP-11)
- **ID del Caso:** CP-11
- **Arquitectura:** RabbitMQ
- **Objetivo:** Validar que mensajes publicados con el consumidor apagado se procesan correctamente cuando este se inicia con posterioridad.
- **Precondiciones:** Cola `pedidos` con mensajes en espera y worker apagado.
- **Datos de Entrada:** Reinicio del consumidor: `docker compose start notification-worker`.
- **Pasos Ejecutados:**
  1. Publicar pedidos con el consumidor inactivo.
  2. Constatar acumulación en estado `Ready`.
  3. Iniciar el servicio consumidor.
  4. Observar el drenado secuencial de la cola y emisión de ACKs.
- **Resultado Esperado:** Consumo diferido integral de todos los mensajes sin omisiones ni alteraciones de orden.
- **Resultado Obtenido:** El worker procesó de inmediato los mensajes acumulados en orden FIFO tan pronto completó el handshake AMQP.
- **Evidencia:** Registros de arranque del worker con consumo de mensajes generados previamente.
- **Estado:** Cumple.
- **Análisis Técnico:** Demuestra la independencia temporal entre emisor y receptor: no requieren concurrencia en tiempo real para completar la transacción.

---

#### Reporte RMQ-12: Recuperación (CP-12)
- **ID del Caso:** CP-12
- **Arquitectura:** RabbitMQ
- **Objetivo:** Comprobar que tras reiniciar el consumidor o el broker, los mensajes no confirmados se recuperan sin pérdida.
- **Precondiciones:** Mensaje en vuelo dentro del consumidor (estado `Unacked`).
- **Datos de Entrada:** Interrupción forzada del contenedor worker durante la ventana de retardo.
- **Pasos Ejecutados:**
  1. Enviar pedido.
  2. Al recibir el mensaje el worker, forzar su terminación antes de que emita el ACK.
  3. Verificar que el canal AMQP se cierra y RabbitMQ devuelve el mensaje a estado `Ready` con la bandera `redelivered=true`.
  4. Levantar nuevamente el worker y verificar el reprocesamiento y confirmación final.
- **Resultado Esperado:** Cero pérdida de mensajes; reentrega transparente tras la falla.
- **Resultado Obtenido:** El mensaje no confirmado regresó a estado `Ready` y fue consumido exitosamente tras el reinicio del contenedor.
- **Evidencia:** Consola de RabbitMQ y log del worker con bandera `redelivered`.
- **Estado:** Cumple.
- **Análisis Técnico:** El protocolo AMQP garantiza la semántica *At-least-once delivery* mediante el ciclo de ACK explícito.

---

### Bloque B: Reportes de Ejecución — Arquitectura 2: Apache Kafka

#### Reporte KFK-01: Creación de pedido vía API (CP-01)
- **ID del Caso:** CP-01
- **Arquitectura:** Kafka
- **Objetivo:** Validar que el endpoint `POST /orders` recibe el pedido completo con `unitPrice`, calcula el total y responde de forma inmediata.
- **Precondiciones:** `orders-api (Kafka)` y broker Kafka en ejecución.
- **Datos de Entrada:** `{"customerName": "Carlos Gómez", "product": "Libro de arquitectura", "quantity": 1, "unitPrice": 85000}`.
- **Pasos Ejecutados:**
  1. Enviar solicitud POST a `http://localhost:3000/orders`.
  2. Verificar validación de parámetros y cálculo aritmético del total ($85,000).
  3. Constatar respuesta HTTP inmediata.
- **Resultado Esperado:** HTTP 202 Accepted con `status: "accepted"`, `orderId`, `eventId`, `topic`, `partition` y `offset`.
- **Resultado Obtenido:** HTTP 202 en 28 ms. Payload: `{"status":"accepted","orderId":"ORD-1729992035000-7712","eventId":"evt-7f12a149-bc91-4cf1-8bc4-9d10f8812c44","topic":"orders.events","partition":0,"offset":"14"}`.
- **Evidencia:** Archivo `evidencias/kafka/kafka-http-post-response.json`.
- **Estado:** Cumple.
- **Análisis Técnico:** La API serializa y emite el evento al cluster Kafka en menos de 30 ms sin esperar a la reacción de los consumidores de inventario, facturación o notificación.

---

#### Reporte KFK-04: Publicación de evento en Kafka (CP-04)
- **ID del Caso:** CP-04
- **Arquitectura:** Kafka
- **Objetivo:** Verificar que se genera y persiste el evento de dominio `OrderCreated` en el tópico `orders.events` con todos los metadatos requeridos.
- **Precondiciones:** Tópico `orders.events` creado con al menos 2 particiones en el cluster Kafka.
- **Datos de Entrada:** Pedido emitido desde `orders-api`.
- **Pasos Ejecutados:**
  1. Publicar pedido desde la API.
  2. Consultar el log de Kafka o Kafka UI.
  3. Inspeccionar el registro: clave (`orderId`), valor JSON y cabeceras.
- **Resultado Esperado:** Evento `OrderCreated` almacenado con `eventId`, `eventType: "OrderCreated"`, `eventVersion: "1.0"`, `occurredAt`, `source: "orders-api"`, `correlationId` y `data` con total calculado.
- **Resultado Obtenido:** Evento persistido en partición 0 con offset 14. Clave asignada: `ORD-1729992035000-7712`. Metadatos de contrato completos.
- **Evidencia:** Archivo `evidencias/kafka/kafka-orders-api-logs.txt` e inspección en Kafka UI.
- **Estado:** Cumple.
- **Análisis Técnico:** Cumplimiento riguroso del patrón Domain Event: el nombre en participio pasado expresa un hecho inmutable consumado en el negocio.

---

#### Reporte KFK-05: Consumo de evento en Kafka (CP-05)
- **ID del Caso:** CP-05
- **Arquitectura:** Kafka
- **Objetivo:** Validar que los tres consumidores independientes reciben y procesan el mismo evento `OrderCreated`.
- **Precondiciones:** `inventory-consumer`, `billing-consumer` y `notification-consumer` activos con grupos funcionales diferenciados.
- **Datos de Entrada:** Evento `OrderCreated` con ID `evt-7f12a149-bc91-4cf1-8bc4-9d10f8812c44`.
- **Pasos Ejecutados:**
  1. Emitir evento en `orders.events`.
  2. Auditar logs de `inventory-consumer` (grupo `inventory-service-group`).
  3. Auditar logs de `billing-consumer` (grupo `billing-service-group`).
  4. Auditar logs de `notification-consumer` (grupo `notification-service-group`).
- **Resultado Esperado:** Los tres consumidores leen el evento del log y ejecutan su lógica de dominio simulada.
- **Resultado Obtenido:**
  - `inventory-consumer`: Reservó 1 unidad de 'Libro de arquitectura'.
  - `billing-consumer`: Generó factura por valor de $85,000.
  - `notification-consumer`: Despachó notificación al cliente 'Carlos Gómez'.
- **Evidencia:** Archivos `kafka-inventory-consumer-logs.txt`, `kafka-billing-consumer-logs.txt` y `kafka-notification-consumer-logs.txt`.
- **Estado:** Cumple.
- **Análisis Técnico:** A diferencia de una cola AMQP donde el mensaje se consume destructivamente por un único worker, Kafka permite el consumo concurrente multipropósito (*Fan-out*) gracias al aislamiento de Consumer Groups.

---

#### Reporte KFK-06: Trazabilidad del flujo (CP-06)
- **ID del Caso:** CP-06
- **Arquitectura:** Kafka
- **Objetivo:** Seguir el mismo pedido a través de `orderId` y `correlationId` en la API, Kafka y los tres consumidores.
- **Precondiciones:** Servicios de la arquitectura Kafka activos.
- **Datos de Entrada:** Pedido con `orderId=ORD-1729992035000-7712` y `correlationId=corr-5a910d92-23f1-46ab-8231-1e901f44a9b1`.
- **Pasos Ejecutados:**
  1. Extraer identificadores del cuerpo de respuesta HTTP.
  2. Localizar el registro en el tópico `orders.events`.
  3. Contrastar los identificadores registrados en los logs de los 3 consumidores.
- **Resultado Esperado:** Correspondencia unívoca de `orderId`, `eventId` y `correlationId` en todo el recorrido.
- **Resultado Obtenido:** Idéntico `correlationId` registrado en la API y en los 3 consumidores funcionales.
- **Evidencia:** Trazas consolidadas en los archivos de evidencia de Kafka.
- **Estado:** Cumple.
- **Análisis Técnico:** La inmutabilidad del log de eventos facilita auditorías forenses y seguimiento de procesos de negocio distribuidos.

---

#### Reporte KFK-07: Responsive: respuesta sin esperar al consumidor (CP-07)
- **ID del Caso:** CP-07
- **Arquitectura:** Kafka
- **Objetivo:** Comprobar que la API responde inmediatamente tras publicar en el log de Kafka sin bloquearse por los consumidores.
- **Precondiciones:** `orders-api` y consumidores conectados.
- **Datos de Entrada:** Solicitud POST `/orders`.
- **Pasos Ejecutados:**
  1. Medir latencia HTTP del endpoint.
  2. Observar los timestamps de inicio y fin en los consumidores (duración acumulada > 2400 ms).
  3. Comparar tiempos.
- **Resultado Esperado:** Latencia HTTP < 60 ms.
- **Resultado Obtenido:** Latencia de respuesta HTTP: 28 ms. Los consumidores iniciaron su procesamiento de manera asincrónica milisegundos después.
- **Evidencia:** Registro `processingTimeMs: 28` en `kafka-http-post-response.json`.
- **Estado:** Cumple.
- **Análisis Técnico:** Cumple la propiedad Responsive: el cliente recibe confirmación de aceptación del pedido en tiempo casi instantáneo.

---

#### Reporte KFK-08: Message-driven: comunicación mediante broker (CP-08)
- **ID del Caso:** CP-08
- **Arquitectura:** Kafka
- **Objetivo:** Demostrar que no existe acoplamiento HTTP entre la API y los consumidores, canalizando todo el intercambio a través del tópico `orders.events`.
- **Precondiciones:** Análisis de la arquitectura en `kafka/docker-compose.yml`.
- **Datos de Entrada:** Inspección de configuración y código.
- **Pasos Ejecutados:**
  1. Verificar ausencia de puertos HTTP y servidores web en los 3 consumidores.
  2. Verificar que `orders-api` solo interactúa con Kafka mediante el protocolo binario en el puerto 9092.
- **Resultado Esperado:** Desacoplamiento total entre emisor y receptores.
- **Resultado Obtenido:** Consumidores no exponen interfaces HTTP; se suscriben pasivamente al tópico de eventos.
- **Evidencia:** Manifiesto `kafka/docker-compose.yml` y código fuente en `kafka/*/src/`.
- **Estado:** Cumple.
- **Análisis Técnico:** La arquitectura es estrictamente dirigida por mensajes y eventos, eliminando cadenas de dependencias síncronas frágiles.

---

#### Reporte KFK-09: Resilient: tolerancia a caída del consumidor (CP-09)
- **ID del Caso:** CP-09
- **Arquitectura:** Kafka
- **Objetivo:** Demostrar que la caída de un consumidor funcional no interrumpe la API ni afecta a los demás consumidores.
- **Precondiciones:** Detener `inventory-consumer` con `docker compose stop inventory-consumer`.
- **Datos de Entrada:** Publicación de pedido con consumidor de inventario inactivo.
- **Pasos Ejecutados:**
  1. Detener el contenedor de inventario.
  2. Enviar pedido vía API.
  3. Constatar respuesta HTTP 202 exitosa.
  4. Verificar que `billing-consumer` y `notification-consumer` procesan el evento con normalidad.
  5. Constatar en Kafka UI que el evento permanece almacenado en el log esperando al grupo de inventario.
- **Resultado Esperado:** Resiliencia total: falla contenida en el dominio afectado sin impacto en el resto del sistema.
- **Resultado Obtenido:** La API respondió normalmente, facturación y notificación consumieron el evento, y el evento quedó disponible para inventario en su offset correspondiente.
- **Evidencia:** Kafka UI mostrando el avance de offsets en `billing` y `notification`, y lag acumulado en `inventory`.
- **Estado:** Cumple.
- **Análisis Técnico:** La persistencia basada en log desacopla los ciclos de vida de cada grupo de consumidores: el fallo de un componente no bloquea el avance de los demás.

---

#### Reporte KFK-10: Elastic: distribución de carga (CP-10)
- **ID del Caso:** CP-10
- **Arquitectura:** Kafka
- **Objetivo:** Demostrar el balanceo de particiones cuando se ejecutan múltiples instancias del mismo consumidor en el mismo grupo.
- **Precondiciones:** Tópico `orders.events` configurado con 2 particiones. Escalar `inventory-consumer` a 2 réplicas.
- **Datos de Entrada:** Publicación de múltiples pedidos con claves diferentes.
- **Pasos Ejecutados:**
  1. Escalar consumidor: `docker compose up -d --scale inventory-consumer=2`.
  2. Verificar rebalanceo del grupo en Kafka UI: Instancia A asignada a Partición 0, Instancia B asignada a Partición 1.
  3. Enviar 4 pedidos y verificar que ambas instancias consumen concurrentemente según la partición asignada.
- **Resultado Esperado:** Distribución paralela de eventos entre las dos réplicas del grupo.
- **Resultado Obtenido:** Ambas instancias consumieron concurrentemente: la réplica 1 procesó los eventos en Partición 0 y la réplica 2 procesó los eventos en Partición 1.
- **Evidencia:** Logs con particiones asignadas y vista de Consumer Groups en Kafka UI.
- **Estado:** Cumple.
- **Análisis Técnico:** Demuestra la elasticidad horizontal en Kafka: la unidad básica de paralelismo y escalabilidad es la partición dentro de un consumer group.

---

#### Reporte KFK-11: Desacoplamiento temporal (CP-11)
- **ID del Caso:** CP-11
- **Arquitectura:** Kafka
- **Objetivo:** Comprobar que un consumidor iniciado después de la publicación de eventos puede leer todos los eventos históricos desde el log.
- **Precondiciones:** Eventos acumulados en el tópico con el consumidor apagado. Suscripción configurada con `fromBeginning: true`.
- **Datos de Entrada:** Inicio diferido del consumidor: `docker compose start inventory-consumer`.
- **Pasos Ejecutados:**
  1. Publicar 3 pedidos sin consumidor activo.
  2. Iniciar el consumidor de inventario.
  3. Constatar lectura secuencial desde el offset inicial (offset 0).
- **Resultado Esperado:** Procesamiento íntegro de todos los eventos históricos almacenados en el log.
- **Resultado Obtenido:** El consumidor leyó y procesó secuencialmente los 3 eventos previos desde el offset 0 sin perder ningún registro.
- **Evidencia:** Logs de arranque con `fromBeginning: true` procesando offsets 0, 1 y 2.
- **Estado:** Cumple.
- **Análisis Técnico:** Propiedad distintiva de Kafka: el consumo es no destructivo. Los eventos permanecen en el log durante el período de retención, permitiendo reprocesamiento y desacoplamiento temporal ilimitado.

---

#### Reporte KFK-12: Recuperación (CP-12)
- **ID del Caso:** CP-12
- **Arquitectura:** Kafka
- **Objetivo:** Comprobar que tras reiniciar un consumidor o el broker, el consumidor reanuda la lectura a partir del último offset confirmado sin pérdida ni duplicidad.
- **Precondiciones:** Offsets confirmados y almacenados en el tópico interno `__consumer_offsets`.
- **Datos de Entrada:** Reinicio forzado del contenedor: `docker compose restart billing-consumer`.
- **Pasos Ejecutados:**
  1. Procesar eventos y constatar commit de offset en Kafka UI.
  2. Reiniciar el contenedor del consumidor.
  3. Enviar nuevo pedido y verificar que la lectura se reanuda a partir del offset comprometido.
- **Resultado Esperado:** Reanudación consistente sin saltos ni reprocesamientos anómalos.
- **Resultado Obtenido:** Tras el reinicio, el consumidor consultó su último offset comprometido en `__consumer_offsets` y procesó únicamente los nuevos eventos entrantes.
- **Evidencia:** Métricas de Current Offset vs Log End Offset en Kafka UI.
- **Estado:** Cumple.
- **Análisis Técnico:** El mecanismo de commit de offsets de Kafka garantiza recuperación determinística y tolerancia a fallos ante caídas intempestivas.

---

## 9. Tabla Resumen de Casos de Prueba

| ID Caso | Caso de Prueba | Tipo de Prueba | RabbitMQ | Apache Kafka | Criterio de Aceptación |
|:---:|---|:---:|:---:|:---:|---|
| **CP-01** | Creación de pedido vía API | Funcional | ✅ Cumple | ✅ Cumple | HTTP 202, validación de payload y generación de ID único |
| **CP-02** | Envío de mensaje a RabbitMQ | Funcional | ✅ Cumple | N/A | Mensaje encolado en `pedidos` con `persistent: true` |
| **CP-03** | Consumo de mensaje (RabbitMQ) | Funcional | ✅ Cumple | N/A | Consumo en worker, simulación y confirmación manual (ACK) |
| **CP-04** | Publicación de evento en Kafka | Funcional | N/A | ✅ Cumple | Evento `OrderCreated` persistido en tópico `orders.events` |
| **CP-05** | Consumo de evento (Kafka) | Funcional | N/A | ✅ Cumple | Recepción en 3 consumidores funcionales independientes (Fan-out) |
| **CP-06** | Trazabilidad del flujo | Funcional | ✅ Cumple | ✅ Cumple | Propagación inmutable de `orderId` y `correlationId` |
| **CP-07** | Responsive | Reactiva | ✅ Cumple | ✅ Cumple | Respuesta HTTP inmediata (<100ms) sin esperar a consumidores |
| **CP-08** | Message-driven | Reactiva | ✅ Cumple | ✅ Cumple | Comunicación exclusiva vía broker; 0 llamadas HTTP directas |
| **CP-09** | Resilient | Reactiva | ✅ Cumple | ✅ Cumple | Tolerancia a caída del consumidor; retención sin pérdida de datos |
| **CP-10** | Elastic | Reactiva | ✅ Cumple | ✅ Cumple | Balanceo de carga horizontal entre múltiples réplicas |
| **CP-11** | Desacoplamiento temporal | Reactiva | ✅ Cumple | ✅ Cumple | Procesamiento diferido de mensajes/eventos acumulados |
| **CP-12** | Recuperación | Reactiva | ✅ Cumple | ✅ Cumple | Reanudación sin pérdida ante reinicio de broker o consumidor |

**Total de casos aplicables por arquitectura:** 10 ejecuciones en RabbitMQ + 10 ejecuciones en Kafka = **20 ejecuciones evaluadas**.

---

## 10. Análisis Comparativo: RabbitMQ vs Apache Kafka

| Criterio de Evaluación | RabbitMQ (Arquitectura 1) | Apache Kafka (Arquitectura 2) | Impacto en Aseguramiento de Calidad (QA) |
|---|---|---|---|
| **Modelo de Comunicación** | Mensajería tradicional basada en colas (*Work Queue* / Smart broker, dumb consumer). | Log distribuido de eventos inmutable (*Event Streaming* / Dumb broker, smart consumer). | En RabbitMQ se audita el estado de la cola; en Kafka se audita la posición del cursor (offset) en el log. |
| **Ciclo de Vida del Mensaje** | **Destructivo:** El mensaje se elimina de la cola una vez confirmado con ACK. | **No destructivo:** Los eventos persisten en disco según política de retención temporal. | Kafka facilita pruebas de regresión y reprocesamiento histórico al poder releer desde el offset 0. |
| **Distribución a Consumidores** | Un mensaje de cola solo es procesado por un único worker (distribución competitiva). | Múltiples consumidores funcionales leen el mismo evento si usan Consumer Groups distintos (*Fan-out*). | Kafka permite incorporar nuevos servicios (p. ej., Auditoría o BI) sin modificar productores ni colas existentes. |
| **Escalabilidad Horizontal (Elastic)** | Basada en workers concurrentes compitiendo con `prefetch(1)`. Granularidad por mensaje. | Basada en particiones. Máximo tantas instancias activas como particiones tenga el tópico. | Requiere que QA diseñe tópicos con múltiples particiones desde el inicio para probar escalamiento. |
| **Manejo de Errores y Caídas** | Mensaje no confirmado regresa a la cabeza de la cola para reentrega (`redelivered=true`). | El consumidor reanuda desde el último offset confirmado en `__consumer_offsets`. | En Kafka el reintento debe gestionarse cuidadosamente para no bloquear el avance de la partición (*Head-of-Line blocking*). |
| **Observabilidad Administrativa** | Consola web integrada simple y directa: contadores `Ready`, `Unacked` y `Total`. | Requiere herramienta visual complementaria (Kafka UI) para inspeccionar particiones y lag de grupos. | RabbitMQ permite verificar colas vacías inmediatamente; Kafka exige comparar `Current Offset` vs `End Offset`. |

---

## 11. Conclusiones Obligatorias

### 11.1 ¿Qué arquitectura es más robusta desde la perspectiva de QA?
Desde la perspectiva de aseguramiento de la calidad, **Apache Kafka resulta sustancialmente más robusta para auditoría, trazabilidad y reproducibilidad de pruebas**, debido a la inmutabilidad de su log de eventos. Mientras que en RabbitMQ un mensaje confirmado con `ack` desaparece definitivamente de la cola (impidiendo su reinspección posterior a menos que se configure una infraestructura externa de trazabilidad o Dead Letter Exchanges), en Kafka el evento permanece registrado con su clave, offset y marca de tiempo exacta. Esto permite a un equipo de QA re-ejecutar pruebas, verificar contratos de eventos en cualquier momento histórico y levantar nuevos entornos de prueba que lean el historial desde el principio (`fromBeginning: true`).

No obstante, para flujos transaccionales simples donde se requiere una cola de tareas transitorias con confirmación inmediata de procesamiento y baja latencia de entrega individual, **RabbitMQ ofrece mayor simplicidad operativa y menor sobrecarga cognitiva en observabilidad de pruebas**, al contar con contadores claros e intuitivos (`Ready` y `Unacked`).

### 11.2 ¿Qué dificultades se presentaron durante las pruebas?
1. **Sincronización Asincrónica en Pruebas de Integración:** A diferencia de las pruebas unitarias síncronas, en sistemas reactivos las aserciones inmediatas sobre el consumidor fallan por condiciones de carrera (*Race Conditions*). Fue necesario implementar esperas activas no bloqueantes (*polling con timeout*) para verificar la llegada del mensaje al worker antes de comprobar los estados finales.
2. **Dependencias de Red Interna en Docker Compose:** El arranque concurrente de los contenedores exige que las aplicaciones productoras y consumidoras implementen políticas de reintentos (*exponential backoff / retry loops*) para tolerar el tiempo de inicialización de los brokers (RabbitMQ y Kafka), evitando fallos por conexiones rechazadas prematuras.
3. **Modelado Estricto de Particiones en Kafka:** Para evidenciar el reparto de carga elástico en Kafka (CP-10), fue indispensable asegurar que el tópico `orders.events` contara con al menos 2 particiones y que la clave de evento (`orderId`) generara distribución balanceada mediante el algoritmo de hash por defecto.
4. **Distinción Conceptual entre Comando y Evento:** Evitar el error común de nombrar los eventos como acciones solicitadas (`CreateOrder`, `SendNotification`) y asegurar la nomenclatura basada en hechos ocurridos (`OrderCreated`).

### 11.3 ¿Qué diferencias reales se evidenciaron entre ambas arquitecturas?
1. **Comportamiento ante Consumo Múltiple:** Mientras que en RabbitMQ para que dos servicios distintos procesen la misma orden se requiere configurar un Exchange de tipo Fanout con colas dedicadas, en Kafka basta con que cada servicio defina su propio `groupId` para recibir una copia idéntica del evento directamente del mismo tópico.
2. **Latencia de Respuesta HTTP vs Duración de Procesamiento:** En ambas arquitecturas se comprobó una reducción dramática del tiempo de respuesta hacia el cliente HTTP (entre 14 ms y 28 ms), frente a procesos de negocio que tardan entre 1000 ms y 3000 ms, ratificando la propiedad *Responsive*.
3. **Mecanismo de Tolerancia a Fallos:** En RabbitMQ la resiliencia depende de la persistencia de la cola y del no envío de ACK prematuro; en Kafka la resiliencia depende de la replicación del log y del registro periódico del commit de offsets.

---

## 12. Enlace al Repositorio e Instrucciones de Reproducción

- **Estructura del Proyecto:**
  ```
  Pruebas de integración Mensajes y eventos/
  ├── rabbitmq/
  │   ├── orders-api/          # API REST Productora RabbitMQ
  │   ├── notification-worker/ # Worker Consumidor con prefetch(1) y manual ack
  │   ├── docker-compose.yml   # Orquestación contenerizada RabbitMQ
  │   └── README.md            # Guía operativa RabbitMQ
  ├── kafka/
  │   ├── orders-api/          # API REST Productora Kafka (Domain Events)
  │   ├── inventory-consumer/  # Consumidor de inventario (Consumer Group 1)
  │   ├── billing-consumer/    # Consumidor de facturación (Consumer Group 2)
  │   ├── notification-consumer/# Consumidor de notificaciones (Consumer Group 3)
  │   ├── docker-compose.yml   # Orquestación Kafka KRaft + Kafka UI + Consumidores
  │   └── README.md            # Guía operativa Kafka
  ├── pruebas/
  │   ├── scripts/             # Scripts automatizados de pruebas (CP-01 a CP-12)
  │   ├── resultados/          # Reportes JSON consolidados y matriz Markdown
  │   └── package.json         # Dependencias de prueba
  ├── evidencias/
  │   ├── rabbitmq/            # Diagramas, logs y trazas de RabbitMQ
  │   └── kafka/               # Diagramas, logs y trazas de Kafka
  ├── informe/
  │   ├── informe-pruebas-integracion.md  # Informe técnico en Markdown
  │   ├── informe-pruebas-integracion.html# Documento para compilación
  │   ├── informe-pruebas-integracion.pdf # Informe final en PDF
  │   └── generate-pdf.js                 # Generador automatizado de PDF
  └── README.md                # Guía maestra de reproducción
  ```

---

## 13. Anexos Técnicos Operativos

### Anexo A: Comandos Clave de Docker Compose
- **Levantar RabbitMQ:** `docker compose -f rabbitmq/docker-compose.yml up -d --build`
- **Escalar Consumidores RabbitMQ:** `docker compose -f rabbitmq/docker-compose.yml up -d --scale notification-worker=2`
- **Levantar Kafka:** `docker compose -f kafka/docker-compose.yml up -d --build`
- **Escalar Consumidores Kafka:** `docker compose -f kafka/docker-compose.yml up -d --scale inventory-consumer=2`
- **Ejecutar Suite de Pruebas Automatizada:** `node pruebas/scripts/run-all-tests.js`
- **Recompilar Informe PDF:** `node informe/generate-pdf.js`
