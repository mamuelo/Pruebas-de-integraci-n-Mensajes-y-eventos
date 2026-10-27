const amqp = require('amqplib');
const os = require('os');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'pedidos';
const PROCESSING_DELAY_MS = parseInt(process.env.PROCESSING_DELAY_MS || '3000', 10);
const WORKER_ID = process.env.WORKER_ID || `worker-${os.hostname().substring(0, 8)}`;

let amqpConnection = null;
let amqpChannel = null;

async function startWorker() {
  const maxRetries = 30;
  const retryInterval = 3000;
  let attempt = 0;

  while (attempt < maxRetries) {
    attempt++;
    try {
      console.log(`[${WORKER_ID}] [INIT] Conectando a RabbitMQ en ${RABBITMQ_URL} (Intento ${attempt}/${maxRetries})...`);
      amqpConnection = await amqp.connect(RABBITMQ_URL);

      amqpConnection.on('error', (err) => {
        console.error(`[${WORKER_ID}] [ERROR] Error en conexión AMQP:`, err.message);
      });

      amqpConnection.on('close', () => {
        console.warn(`[${WORKER_ID}] [WARN] Conexión AMQP cerrada. Reintentando reconexión en ${retryInterval / 1000}s...`);
        setTimeout(startWorker, retryInterval);
      });

      amqpChannel = await amqpConnection.createChannel();

      // Declarar cola durable idéntica a la productora
      await amqpChannel.assertQueue(QUEUE_NAME, { durable: true });

      // Configuración obligatoria: prefetch 1 para reparto equitativo entre workers
      await amqpChannel.prefetch(1);

      console.log(`[${WORKER_ID}] [READY] Suscrito a cola '${QUEUE_NAME}'. Esperando mensajes con prefetch=1...`);

      // Consumo de mensajes con confirmación manual (ack)
      amqpChannel.consume(QUEUE_NAME, async (msg) => {
        if (!msg) {
          console.warn(`[${WORKER_ID}] [WARN] Mensaje nulo recibido del broker.`);
          return;
        }

        const receiveTime = new Date().toISOString();
        let payload;

        try {
          payload = JSON.parse(msg.content.toString());
        } catch (parseErr) {
          console.error(`[${WORKER_ID}] [ERROR] Fallo al parsear contenido JSON:`, parseErr.message);
          // Si el mensaje es corrupto, se descarta para no bloquear la cola
          amqpChannel.nack(msg, false, false);
          return;
        }

        const { orderId, customerName, product, quantity, correlationId, createdAt } = payload;

        console.log(
          `[${WORKER_ID}] [RECEIVED] [${receiveTime}] [orderId=${orderId}] [correlationId=${correlationId}] ` +
          `Mensaje recibido: Cliente='${customerName}', Producto='${product}', Cantidad=${quantity}. ` +
          `Iniciando procesamiento simulado (${PROCESSING_DELAY_MS}ms)...`
        );

        try {
          // Simulación de procesamiento (espera de 2 a 5 segundos para observar estado Unacked)
          await new Promise((resolve) => setTimeout(resolve, PROCESSING_DELAY_MS));

          const finishTime = new Date().toISOString();
          console.log(
            `[${WORKER_ID}] [PROCESSED] [${finishTime}] [orderId=${orderId}] [correlationId=${correlationId}] ` +
            `Notificación enviada exitosamente al cliente '${customerName}'. Confirmando mensaje (ACK)...`
          );

          // Confirmación manual (ACK)
          amqpChannel.ack(msg);
          console.log(`[${WORKER_ID}] [ACK_DONE] [orderId=${orderId}] Mensaje confirmado y liberado de RabbitMQ.`);
        } catch (processErr) {
          console.error(`[${WORKER_ID}] [ERROR] Error durante el procesamiento de ${orderId}:`, processErr);
          // Reencolar en caso de error transitorio
          amqpChannel.nack(msg, false, true);
        }
      }, { noAck: false }); // noAck: false para requerir ack manual obligatorio

      return;
    } catch (err) {
      console.error(`[${WORKER_ID}] [RETRY] Fallo al inicializar worker: ${err.message}. Reintentando en ${retryInterval / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, retryInterval));
    }
  }

  console.error(`[${WORKER_ID}] [FATAL] No se pudo conectar al broker tras superar los intentos máximos.`);
}

// Iniciar worker
startWorker().catch(console.error);

// Manejo de señales de terminación para cierre ordenado
process.on('SIGTERM', async () => {
  console.log(`[${WORKER_ID}] [SHUTDOWN] Señal SIGTERM recibida. Cerrando canales y conexiones...`);
  try {
    if (amqpChannel) await amqpChannel.close();
    if (amqpConnection) await amqpConnection.close();
  } catch (e) {
    // ignorar en salida
  }
  process.exit(0);
});
