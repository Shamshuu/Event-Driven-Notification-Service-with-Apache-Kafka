import { Kafka, Producer, LogEntry, logLevel } from 'kafkajs';
import { logger } from './logger';
import dotenv from 'dotenv';

dotenv.config();

const brokerList = process.env.KAFKA_BROKER_LIST
  ? process.env.KAFKA_BROKER_LIST.split(',')
  : ['localhost:9092'];

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
  clientId: 'notification-api-service',
  brokers: brokerList,
  logCreator: winstonLogCreator,
});

export const producer: Producer = kafka.producer();

export async function connectKafkaProducer(): Promise<void> {
  try {
    logger.info('Connecting Kafka Producer...');
    await producer.connect();
    logger.info('Kafka Producer connected successfully.');
  } catch (error) {
    logger.error('Failed to connect Kafka Producer:', error);
    throw error;
  }
}

export async function disconnectKafkaProducer(): Promise<void> {
  try {
    await producer.disconnect();
    logger.info('Kafka Producer disconnected.');
  } catch (error) {
    logger.error('Error disconnecting Kafka Producer:', error);
  }
}
