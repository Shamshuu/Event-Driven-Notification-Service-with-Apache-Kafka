import { Request, Response } from 'express';
import { db } from '../config/db';
import { logger } from '../config/logger';
import { GetNotificationsSchema, MarkReadSchema } from '../schemas/event.schema';
import { ResultSetHeader, RowDataPacket } from 'mysql2';

export async function getUserNotifications(req: Request, res: Response): Promise<void> {
  try {
    const parseResult = GetNotificationsSchema.safeParse(req.params);
    if (!parseResult.success) {
      logger.warn(`Invalid GET user notifications params: ${JSON.stringify(parseResult.error.format())}`);
      res.status(400).json({
        error: 'Bad Request',
        message: 'Invalid userId parameter',
        details: parseResult.error.format(),
      });
      return;
    }

    const { userId } = parseResult.data;

    logger.info(`Fetching unread notifications for user: ${userId}`);

    const [rows] = await db.query<RowDataPacket[]>(
      `SELECT notification_id, recipient_user_id, event_type, message_content, status, created_at 
       FROM notifications 
       WHERE recipient_user_id = ? AND status = 'unread' 
       ORDER BY created_at DESC`,
      [userId]
    );

    res.status(200).json(rows);
  } catch (error) {
    logger.error('Error fetching user notifications:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve notifications from database',
    });
  }
}

export async function markNotificationAsRead(req: Request, res: Response): Promise<void> {
  try {
    const parseResult = MarkReadSchema.safeParse(req.params);
    if (!parseResult.success) {
      logger.warn(`Invalid PATCH notification params: ${JSON.stringify(parseResult.error.format())}`);
      res.status(400).json({
        error: 'Bad Request',
        message: 'Invalid notificationId parameter',
        details: parseResult.error.format(),
      });
      return;
    }

    const { notificationId } = parseResult.data;

    logger.info(`Marking notification as read: ${notificationId}`);

    const [result] = await db.query<ResultSetHeader>(
      `UPDATE notifications 
       SET status = 'read' 
       WHERE notification_id = ?`,
      [notificationId]
    );

    if (result.affectedRows === 0) {
      logger.warn(`Notification not found for update: ${notificationId}`);
      res.status(404).json({
        error: 'Not Found',
        message: 'Notification not found',
      });
      return;
    }

    logger.info(`Notification marked as read successfully: ${notificationId}`);
    res.status(204).end();
  } catch (error) {
    logger.error('Error marking notification as read:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to update notification status in database',
    });
  }
}
