const { Kafka, LogLevel } = require('kafkajs');
const os = require('os');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || 'inventory-service-group';
const CONSUMER_INSTANCE_ID = process.env.CONSUMER_INSTANCE_ID || `inventory-worker-${os.hostname().substring(0, 8)}`;

const kafka = new Kafka({
  clientId: CONSUMER_INSTANCE_ID,
  brokers: [KAFKA_BROKER],
  logLevel: LogLevel.WARN,
  retry: {
    initialRetryTime: 1000,
    retries: 30
  }
});

const consumer = kafka.consumer({
  groupId: KAFKA_GROUP_ID,
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});

async function startConsumer() {
  const maxRetries = 30;
  const retryInterval = 3000;
  let attempt = 0;

  while (attempt < maxRetries) {
    attempt++;
    try {
      console.log(`[${CONSUMER_INSTANCE_ID}] [INIT] Conectando a Kafka broker ${KAFKA_BROKER} (Intento ${attempt}/${maxRetries})...`);
      await consumer.connect();
      console.log(`[${CONSUMER_INSTANCE_ID}] [READY] Conectado. Suscribiendo al tópico '${KAFKA_TOPIC}' (fromBeginning: true)...`);

      await consumer.subscribe({
        topic: KAFKA_TOPIC,
        fromBeginning: true
      });

      console.log(`[${CONSUMER_INSTANCE_ID}] [LISTENING] Grupo '${KAFKA_GROUP_ID}' activo y esperando eventos...`);

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const timestamp = new Date().toISOString();
          let event;
          try {
            event = JSON.parse(message.value.toString());
          } catch (e) {
            console.error(`[${CONSUMER_INSTANCE_ID}] [ERROR] Mensaje con formato no JSON:`, message.value?.toString());
            return;
          }

          const eventType = event.eventType;
          const eventId = event.eventId;
          const correlationId = event.correlationId;
          const orderData = event.data || {};
          const orderId = orderData.orderId;

          console.log(
            `[${CONSUMER_INSTANCE_ID}] [RECEIVED] [${timestamp}] [orderId=${orderId}] [correlationId=${correlationId}] ` +
            `Evento='${eventType}' eventId='${eventId}' topic=${topic} partition=${partition} offset=${message.offset}`
          );

          if (eventType === 'OrderCreated') {
            // Simular lógica de inventario
            console.log(
              `[${CONSUMER_INSTANCE_ID}] [ACTION] Simulando reserva de ${orderData.quantity} unidad(es) ` +
              `del producto '${orderData.product}' para la orden '${orderId}'...`
            );

            // Simulación de procesamiento asincrónico
            await new Promise((resolve) => setTimeout(resolve, 800));

            console.log(
              `[${CONSUMER_INSTANCE_ID}] [SUCCESS] [orderId=${orderId}] Inventario reservado con éxito. ` +
              `Offset ${message.offset} procesado.`
            );
          } else {
            console.log(`[${CONSUMER_INSTANCE_ID}] [IGNORE] Evento ignorado (tipo no gestionado): ${eventType}`);
          }
        }
      });

      return;
    } catch (err) {
      console.error(`[${CONSUMER_INSTANCE_ID}] [RETRY] Error al conectar/iniciar consumidor: ${err.message}. Reintentando en ${retryInterval / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, retryInterval));
    }
  }

  console.error(`[${CONSUMER_INSTANCE_ID}] [FATAL] Consumidor no pudo iniciar tras agotar reintentos.`);
}

startConsumer().catch(console.error);

process.on('SIGTERM', async () => {
  console.log(`[${CONSUMER_INSTANCE_ID}] [SHUTDOWN] Señal SIGTERM recibida. Desconectando consumidor...`);
  try {
    await consumer.disconnect();
  } catch (e) {}
  process.exit(0);
});
