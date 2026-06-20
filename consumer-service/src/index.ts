import { Kafka, logLevel, LogEntry } from 'kafkajs';
import { checkDbConnection, db } from './config/db';
import { logger } from './config/logger';
import { processNotificationEvent } from './services/notification.service';
import dotenv from 'dotenv';

dotenv.config();

const brokerList = process.env.KAFKA_BROKER_LIST
  ? process.env.KAFKA_BROKER_LIST.split(',')
  : ['localhost:9092'];
const groupId = process.env.KAFKA_GROUP_ID || 'notification-consumer-group';
const topic = process.env.KAFKA_TOPIC || 'user-activity';

const toWinstonLogLevel = (level: logLevel): string => {
  switch (level) {
    case logLevel.ERROR:
    case logLevel.NOTHING:
      return 'error';
    case logLevel.WARN:
      return 'warn';
    case logLevel.INFO:
      return 'info';
    case logLevel.DEBUG:
      return 'debug';
    default:
      return 'info';
  }
};

const winstonLogCreator = () => {
  return ({ namespace, level, label, log }: LogEntry) => {
    const { message, ...extra } = log;
    logger.log({
      level: toWinstonLogLevel(level),
      message: `${label} [${namespace}] ${message}`,
      ...extra,
    });
  };
};

const kafka = new Kafka({
  clientId: 'notification-consumer-service',
  brokers: brokerList,
  logCreator: winstonLogCreator,
});

const consumer = kafka.consumer({ groupId });

async function startConsumer(): Promise<void> {
  try {
    // 1. Confirm database connection
    const dbConnected = await checkDbConnection();
    if (!dbConnected) {
      logger.error('Startup failed: Database connection could not be established.');
      process.exit(1);
    }

    // 2. Connect Kafka Consumer
    logger.info('Connecting Kafka Consumer...');
    await consumer.connect();
    logger.info('Kafka Consumer connected.');

    // 3. Subscribe to the activity events topic
    logger.info(`Subscribing to Kafka topic: ${topic}`);
    await consumer.subscribe({ topic, fromBeginning: true });

    // 4. Start consuming loop
    await consumer.run({
      eachMessage: async ({ message }) => {
        const rawMessage = message.value?.toString();
        if (rawMessage) {
          // If processor throws, KafkaJS will retry the message batch automatically
          await processNotificationEvent(rawMessage);
        }
      },
    });

    logger.info('Kafka consumer loop is running.');

    // 5. Setup graceful shutdown hooks
    const gracefulShutdown = async (signal: string) => {
      logger.info(`Received ${signal}. Initiating graceful shutdown...`);
      try {
        await consumer.disconnect();
        logger.info('Kafka Consumer disconnected.');
        await db.end();
        logger.info('Database pool closed.');
        logger.info('Shutdown complete. Exiting.');
        process.exit(0);
      } catch (err) {
        logger.error('Error during shutdown operations:', err);
        process.exit(1);
      }
    };

    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  } catch (error) {
    logger.error('Error starting Consumer Service:', error);
    process.exit(1);
  }
}

startConsumer();
