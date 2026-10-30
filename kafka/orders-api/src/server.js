const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const crypto = require('crypto');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const NUM_PARTITIONS = parseInt(process.env.NUM_PARTITIONS || '2', 10);

const kafka = new Kafka({
  clientId: 'orders-api',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 1000,
    retries: 30
  }
});

const producer = kafka.producer({
  allowAutoTopicCreation: true
});

const admin = kafka.admin();
let isConnected = false;

// Conectar productor y asegurar que el tópico existe con al menos 2 particiones
async function initKafka() {
  const maxRetries = 30;
  const retryInterval = 3000;
  let attempt = 0;

  while (attempt < maxRetries) {
    attempt++;
    try {
      console.log(`[orders-api] [INIT] Conectando a Kafka en ${KAFKA_BROKER} (Intento ${attempt}/${maxRetries})...`);
      await producer.connect();
      await admin.connect();

      // Verificar / crear tópico con particiones requeridas
      const topics = await admin.listTopics();
      if (!topics.includes(KAFKA_TOPIC)) {
        console.log(`[orders-api] [ADMIN] Creando tópico '${KAFKA_TOPIC}' con ${NUM_PARTITIONS} particiones...`);
        await admin.createTopics({
          topics: [
            {
              topic: KAFKA_TOPIC,
              numPartitions: NUM_PARTITIONS,
              replicationFactor: 1
            }
          ]
        });
        console.log(`[orders-api] [ADMIN] Tópico '${KAFKA_TOPIC}' creado exitosamente.`);
      } else {
        console.log(`[orders-api] [ADMIN] Tópico '${KAFKA_TOPIC}' ya existe en el cluster.`);
      }

      await admin.disconnect();
      isConnected = true;
      console.log(`[orders-api] [READY] Conectado exitosamente a Kafka y listo para emitir eventos.`);
      return;
    } catch (err) {
      console.error(`[orders-api] [RETRY] Error al conectar a Kafka: ${err.message}. Reintentando en ${retryInterval / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, retryInterval));
    }
  }

  console.error('[orders-api] [FATAL] No se pudo conectar a Kafka tras agotar los reintentos.');
}

// Endpoint de salud
app.get('/health', (req, res) => {
  res.status(isConnected ? 200 : 503).json({
    service: 'orders-api (Kafka)',
    status: isConnected ? 'UP' : 'DEGRADED',
    brokerConnected: isConnected,
    topic: KAFKA_TOPIC,
    timestamp: new Date().toISOString()
  });
});

// Endpoint obligatorio POST /orders
app.post('/orders', async (req, res) => {
  const startTime = Date.now();
  const { customerName, product, quantity, unitPrice } = req.body || {};

  // Validaciones de datos de entrada
  if (!customerName || typeof customerName !== 'string' || customerName.trim() === '') {
    return res.status(400).json({
      error: 'BadRequest',
      message: "El campo 'customerName' es obligatorio y debe ser texto no vacío."
    });
  }

  if (!product || typeof product !== 'string' || product.trim() === '') {
    return res.status(400).json({
      error: 'BadRequest',
      message: "El campo 'product' es obligatorio y debe ser texto no vacío."
    });
  }

  const parsedQty = Number(quantity);
  if (!Number.isInteger(parsedQty) || parsedQty <= 0) {
    return res.status(400).json({
      error: 'BadRequest',
      message: "El campo 'quantity' debe ser un número entero positivo mayor a cero."
    });
  }

  const parsedPrice = Number(unitPrice);
  if (isNaN(parsedPrice) || parsedPrice <= 0) {
    return res.status(400).json({
      error: 'BadRequest',
      message: "El campo 'unitPrice' debe ser un valor numérico positivo mayor a cero."
    });
  }

  if (!isConnected) {
    return res.status(503).json({
      error: 'ServiceUnavailable',
      message: 'El broker de Kafka no está disponible actualmente. Reintente en unos momentos.'
    });
  }

  // Cálculo del total
  const total = parsedQty * parsedPrice;

  // Generación de identificadores de dominio
  const orderId = `ORD-${Date.now()}-${Math.floor(1000 + Math.random() * 9000)}`;
  const eventId = `evt-${crypto.randomUUID()}`;
  const correlationId = `corr-${crypto.randomUUID()}`;
  const occurredAt = new Date().toISOString();

  // Construcción del evento de dominio OrderCreated según especificación estricta
  const orderCreatedEvent = {
    eventId,
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt,
    source: 'orders-api',
    correlationId,
    data: {
      orderId,
      customerName: customerName.trim(),
      product: product.trim(),
      quantity: parsedQty,
      unitPrice: parsedPrice,
      total
    }
  };

  try {
    // Publicación asincrónica en Kafka con key=orderId para hashing de partición
    const recordMetadata = await producer.send({
      topic: KAFKA_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderCreatedEvent),
          headers: {
            correlationId,
            eventType: 'OrderCreated',
            publishedAt: occurredAt
          }
        }
      ]
    });

    const responseDurationMs = Date.now() - startTime;
    const partition = recordMetadata[0]?.partition;
    const offset = recordMetadata[0]?.baseOffset;

    console.log(
      `[orders-api] [${occurredAt}] [orderId=${orderId}] [correlationId=${correlationId}] ` +
      `Evento 'OrderCreated' publicado en '${KAFKA_TOPIC}' (partición=${partition}, offset=${offset}) en ${responseDurationMs}ms.`
    );

    // Respuesta inmediata al cliente sin esperar a los consumidores funcionales (propiedad Responsive)
    return res.status(202).json({
      status: 'accepted',
      message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
      orderId,
      eventId,
      correlationId,
      topic: KAFKA_TOPIC,
      partition,
      offset,
      processingTimeMs: responseDurationMs
    });
  } catch (err) {
    console.error(`[orders-api] [ERROR] Error al publicar evento en Kafka para orden ${orderId}:`, err);
    return res.status(500).json({
      error: 'InternalServerError',
      message: 'Error al publicar el evento de dominio en Apache Kafka.'
    });
  }
});

app.listen(PORT, async () => {
  console.log(`[orders-api] [STARTUP] Servidor HTTP (Kafka) escuchando en puerto ${PORT}`);
  initKafka().catch(console.error);
});
