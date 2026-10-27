# Matriz Resumen de Ejecución de Pruebas de Integración

**Fecha de Ejecución:** 2026-10-27T01:49:15.130Z  
**Total Casos Evaluados:** 20 | **Cumple:** 2 | **No Cumple:** 0 | **Pendientes:** 18

| ID | Caso de Prueba | Arquitectura | Tipo | Estado | Resultado Obtenido |
|---|---|---|---|---|---|
| **CP-01** | Validar que el endpoint POST /orders recibe e... | RabbitMQ | Funcional | ⏳ Pendiente | Entorno de contenedores pendiente de despliegue en host.... |
| **CP-02** | Verificar que la creación de un pedido public... | RabbitMQ | Funcional | ⏳ Pendiente | Broker RabbitMQ pendiente de inicialización en puerto 5672.... |
| **CP-03** | Validar que el worker notification-worker ext... | RabbitMQ | Funcional | ⏳ Pendiente | Contenedor notification-worker pendiente de ejecución.... |
| **CP-06** | Seguir un mismo pedido (orderId y correlation... | RabbitMQ | Funcional | ⏳ Pendiente | Pendiente de despliegue de contenedores.... |
| **CP-07** | Demostrar que la API responde inmediatamente ... | RabbitMQ | Reactiva | ⏳ Pendiente | Pendiente de validación con API en ejecución.... |
| **CP-08** | Verificar que no existe invocación directa HT... | RabbitMQ | Reactiva | ✅ Cumple | notification-worker no expone puertos ni API REST. orders-api interact... |
| **CP-09** | Verificar que la detención del worker no caus... | RabbitMQ | Reactiva | ⏳ Pendiente | Pendiente de prueba de parada de contenedor.... |
| **CP-10** | Demostrar el reparto de carga equitativo entr... | RabbitMQ | Reactiva | ⏳ Pendiente | Pendiente de ejecución de comando de escalamiento en Docker Compose.... |
| **CP-11** | Verificar que mensajes acumulados mientras el... | RabbitMQ | Reactiva | ⏳ Pendiente | Pendiente de prueba controlada de parada y arranque.... |
| **CP-12** | Comprobar que si el worker o el broker se rei... | RabbitMQ | Reactiva | ⏳ Pendiente | Pendiente de prueba de terminación forzada y recuperación.... |
| **CP-01** | Validar que el endpoint POST /orders recibe e... | Kafka | Funcional | ⏳ Pendiente | Contenedores de Kafka pendientes de despliegue.... |
| **CP-04** | Verificar que la creación del pedido genera y... | Kafka | Funcional | ⏳ Pendiente | Broker Kafka pendiente de inicialización en puerto 29092.... |
| **CP-05** | Validar que los tres consumidores independien... | Kafka | Funcional | ⏳ Pendiente | Pendiente de ejecución de contenedores consumidores.... |
| **CP-06** | Seguir un mismo pedido a través de orderId y ... | Kafka | Funcional | ⏳ Pendiente | Pendiente de despliegue de contenedores.... |
| **CP-07** | Comprobar que la API responde de forma asincr... | Kafka | Reactiva | ⏳ Pendiente | Pendiente de ejecución de servicios.... |
| **CP-08** | Verificar la ausencia de comunicación directa... | Kafka | Reactiva | ✅ Cumple | Consumidores carecen de endpoints HTTP. Toda la interacción ocurre a t... |
| **CP-09** | Comprobar que si uno o varios consumidores se... | Kafka | Reactiva | ⏳ Pendiente | Pendiente de prueba de detención de consumidor.... |
| **CP-10** | Demostrar el balanceo de particiones cuando s... | Kafka | Reactiva | ⏳ Pendiente | Pendiente de prueba de escalamiento en Docker Compose.... |
| **CP-11** | Validar que un consumidor iniciado con poster... | Kafka | Reactiva | ⏳ Pendiente | Pendiente de prueba de arranque diferido.... |
| **CP-12** | Comprobar que tras reiniciar un consumidor o ... | Kafka | Reactiva | ⏳ Pendiente | Pendiente de prueba de reinicio de broker y consumidor.... |
