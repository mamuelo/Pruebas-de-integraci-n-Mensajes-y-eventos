const express = require('express');
const amqp = require('amqplib');
const crypto = require('crypto');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'pedidos';

let amqpConnection = null;
let amqpChannel = null;
let isConnected = false;

// Función con reintentos para conectar al broker RabbitMQ
async function connectRabbitMQ() {
  const maxRetries = 30;
  const retryInterval = 3000;
  let attempt = 0;

  while (attempt < maxRetries) {
    attempt++;
    try {
      console.log(`[orders-api] [INIT] Intentando conectar a RabbitMQ en ${RABBITMQ_URL} (Intento ${attempt}/${maxRetries})...`);
      amqpConnection = await amqp.connect(RABBITMQ_URL);
      
      amqpConnection.on('error', (err) => {
        console.error('[orders-api] [ERROR] Error en conexión AMQP:', err.message);
        isConnected = false;
      });

      amqpConnection.on('close', () => {
        console.warn('[orders-api] [WARN] Conexión AMQP cerrada. Reintentando reconexión...');
        isConnected = false;
        setTimeout(connectRabbitMQ, retryInterval);
      });

      amqpChannel = await amqpConnection.createChannel();
      
      // Declarar cola durable según especificación
      await amqpChannel.assertQueue(QUEUE_NAME, { durable: true });
      isConnected = true;
      console.log(`[orders-api] [READY] Conectado exitosamente a RabbitMQ. Cola '${QUEUE_NAME}' verificada/declarada.`);
      return;
    } catch (error) {
      console.error(`[orders-api] [RETRY] Fallo de conexión: ${error.message}. Reintentando en ${retryInterval / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, retryInterval));
    }
  }

  console.error('[orders-api] [FATAL] No se pudo conectar a RabbitMQ tras los reintentos máximos.');
}

// Endpoint de salud para verificar disponibilidad de API y Broker
app.get('/health', (req, res) => {
  res.status(isConnected ? 200 : 503).json({
    service: 'orders-api (RabbitMQ)',
    status: isConnected ? 'UP' : 'DEGRADED',
    brokerConnected: isConnected,
    queue: QUEUE_NAME,
    timestamp: new Date().toISOString()
  });
});

// Endpoint obligatorio POST /orders
app.post('/orders', async (req, res) => {
  const startTime = Date.now();
  const { customerName, product, quantity } = req.body || {};

  // Validación de datos de entrada
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
      message: "El campo 'quantity' debe ser un entero positivo mayor a cero."
    });
  }

  if (!isConnected || !amqpChannel) {
    return res.status(503).json({
      error: 'ServiceUnavailable',
      message: 'El broker RabbitMQ no está disponible actualmente. Reintente en unos momentos.'
    });
  }

  // Generación de identificadores de pedido y correlación
  const orderId = `ORD-${Date.now()}-${Math.floor(1000 + Math.random() * 9000)}`;
  const correlationId = `corr-${crypto.randomUUID()}`;
  const createdAt = new Date().toISOString();

  // Construcción del mensaje de pedido
  const messagePayload = {
    type: 'ProcessOrder',
    orderId,
    customerName: customerName.trim(),
    product: product.trim(),
    quantity: parsedQty,
    createdAt,
    correlationId
  };

  try {
    // Publicación asincrónica en cola con persistencia
    const published = amqpChannel.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify(messagePayload)),
      {
        persistent: true,
        correlationId,
        messageId: orderId,
        timestamp: Date.now(),
        contentType: 'application/json'
      }
    );

    const responseDurationMs = Date.now() - startTime;

    console.log(
      `[orders-api] [${createdAt}] [orderId=${orderId}] [correlationId=${correlationId}] ` +
      `Mensaje publicado en cola '${QUEUE_NAME}' (publicado=${published}) en ${responseDurationMs}ms.`
    );

    // Respuesta inmediata al cliente sin esperar al consumidor (propiedad Responsive)
    return res.status(202).json({
      status: 'accepted',
      message: 'Pedido recibido y enviado a RabbitMQ',
      orderId,
      correlationId,
      queue: QUEUE_NAME,
      processingTimeMs: responseDurationMs
    });
  } catch (err) {
    console.error(`[orders-api] [ERROR] Error al publicar pedido ${orderId}:`, err);
    return res.status(500).json({
      error: 'InternalServerError',
      message: 'Error interno al publicar el mensaje en RabbitMQ.'
    });
  }
});

app.listen(PORT, async () => {
  console.log(`[orders-api] [STARTUP] Servidor HTTP escuchando en puerto ${PORT}`);
  connectRabbitMQ().catch(console.error);
});
