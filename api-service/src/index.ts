import app from './app';
import { checkDbConnection, db } from './config/db';
import { connectKafkaProducer, disconnectKafkaProducer } from './config/kafka';
import { logger } from './config/logger';

const PORT = parseInt(process.env.API_PORT || '3000', 10);

async function startServer(): Promise<void> {
  try {
    // 1. Check Database connection
    const dbConnected = await checkDbConnection();
    if (!dbConnected) {
      logger.error('Startup failed: Database connection could not be established.');
      process.exit(1);
    }

    // 2. Connect Kafka Producer
    await connectKafkaProducer();

    // 3. Start Express server
    const server = app.listen(PORT, () => {
      logger.info(`API Service is running on port ${PORT}`);
    });

    // 4. Handle clean shutdown
    const gracefulShutdown = async (signal: string) => {
      logger.info(`Received ${signal}. Starting graceful shutdown...`);

      server.close(async () => {
        logger.info('Express server closed.');
        try {
          await disconnectKafkaProducer();
          await db.end();
          logger.info('Database pool closed.');
          logger.info('Graceful shutdown completed. Exiting.');
          process.exit(0);
        } catch (err) {
          logger.error('Error during shutdown operations:', err);
          process.exit(1);
        }
      });
    };

    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  } catch (error) {
    logger.error('Error starting API Service:', error);
    process.exit(1);
  }
}

startServer();
