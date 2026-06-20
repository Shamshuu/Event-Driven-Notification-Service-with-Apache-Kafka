import { Router } from 'express';
import { publishUserActivityEvent } from '../controllers/event.controller';
import { getUserNotifications, markNotificationAsRead } from '../controllers/notification.controller';

const router = Router();

// Endpoint to receive user activity and publish to Kafka
router.post('/user-activity-events', publishUserActivityEvent);

// Endpoint to retrieve all unread notifications for a user
router.get('/users/:userId/notifications', getUserNotifications);

// Endpoint to mark a specific notification as read
router.patch('/notifications/:notificationId/read', markNotificationAsRead);

export default router;
