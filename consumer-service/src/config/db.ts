import mysql from 'mysql2/promise';
import { logger } from './logger';
import dotenv from 'dotenv';

dotenv.config();

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST || 'localhost',
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD || 'notification_secret_pass',
  database: process.env.MYSQL_DATABASE || 'notification_db',
  port: parseInt(process.env.MYSQL_PORT || '3306', 10),
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

export const db = pool;

export async function checkDbConnection(): Promise<boolean> {
  try {
    const connection = await pool.getConnection();
    logger.info('MySQL connection pool established successfully.');
    connection.release();
    return true;
  } catch (error) {
    logger.error('Failed to connect to MySQL database:', error);
    return false;
  }
}
