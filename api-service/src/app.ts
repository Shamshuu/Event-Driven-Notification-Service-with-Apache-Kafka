import express, { Request, Response } from 'express';
import cors from 'cors';
import { db } from './config/db';
import apiRouter from './routes/api.routes';
import { logger } from './config/logger';

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Request logging middleware
app.use((req, res, next) => {
  logger.http(`${req.method} ${req.url}`);
  next();
});

// Robust Healthcheck Endpoint
app.get('/health', async (req: Request, res: Response) => {
  try {
    // Check if we can run a simple query on MySQL
    await db.query('SELECT 1');
    res.status(200).json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      services: {
        database: 'connected',
      },
    });
  } catch (error) {
    logger.error('Healthcheck failed: Database connection issue.', error);
    res.status(500).json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      services: {
        database: 'disconnected',
      },
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// API Routes
app.use('/api', apiRouter);

// Global Error Handler
app.use((err: any, req: Request, res: Response, next: any) => {
  logger.error('Unhandled API error:', err);
  res.status(err.status || 500).json({
    error: 'Internal Server Error',
    message: err.message || 'An unexpected error occurred',
  });
});

export default app;
