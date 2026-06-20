import request from 'supertest';
import app from '../src/app';
import { db } from '../src/config/db';
import { producer } from '../src/config/kafka';

// Mock the DB and Kafka configuration so tests can run in isolation
jest.mock('../src/config/db', () => ({
  db: {
    query: jest.fn(),
  },
  checkDbConnection: jest.fn().mockResolvedValue(true),
}));

jest.mock('../src/config/kafka', () => ({
  producer: {
    send: jest.fn().mockResolvedValue([]),
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
  },
  connectKafkaProducer: jest.fn().mockResolvedValue(undefined),
  disconnectKafkaProducer: jest.fn().mockResolvedValue(undefined),
}));

describe('API Service Endpoints', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('POST /api/user-activity-events', () => {
    it('should accept valid user_liked_post events and return 202', async () => {
      const response = await request(app)
        .post('/api/user-activity-events')
        .send({
          event_type: 'user_liked_post',
          payload: {
            user_id: 'liker-123',
            post_id: 'post-456',
            liked_by_user_id: 'owner-789',
          },
        });

      expect(response.status).toBe(202);
      expect(response.body).toHaveProperty('message', 'Event published successfully');
      expect(response.body).toHaveProperty('event_id');
      expect(producer.send).toHaveBeenCalledTimes(1);
    });

    it('should accept valid user_commented events and return 202', async () => {
      const response = await request(app)
        .post('/api/user-activity-events')
        .send({
          event_type: 'user_commented',
          payload: {
            user_id: 'commenter-123',
            post_id: 'post-456',
            comment_text: 'Awesome post!',
            recipient_id: 'owner-789',
          },
        });

      expect(response.status).toBe(202);
      expect(response.body).toHaveProperty('message', 'Event published successfully');
      expect(response.body).toHaveProperty('event_id');
      expect(producer.send).toHaveBeenCalledTimes(1);
    });

    it('should reject requests with missing payload fields with 400', async () => {
      const response = await request(app)
        .post('/api/user-activity-events')
        .send({
          event_type: 'user_liked_post',
          payload: {
            user_id: 'liker-123',
            // post_id is missing
            liked_by_user_id: 'owner-789',
          },
        });

      expect(response.status).toBe(400);
      expect(producer.send).not.toHaveBeenCalled();
    });

    it('should reject requests with unsupported event types with 400', async () => {
      const response = await request(app)
        .post('/api/user-activity-events')
        .send({
          event_type: 'user_followed',
          payload: {
            user_id: 'user-1',
            recipient_id: 'user-2',
          },
        });

      expect(response.status).toBe(400);
    });
  });

  describe('GET /api/users/:userId/notifications', () => {
    it('should return 200 and unread notifications for a user', async () => {
      const mockNotifications = [
        {
          notification_id: 'noti-1',
          recipient_user_id: 'user-123',
          event_type: 'user_liked_post',
          message_content: 'Your post was liked by user-456.',
          status: 'unread',
          created_at: new Date().toISOString(),
        },
      ];

      (db.query as jest.Mock).mockResolvedValue([mockNotifications]);

      const response = await request(app).get('/api/users/user-123/notifications');

      expect(response.status).toBe(200);
      expect(response.body).toEqual(mockNotifications);
      expect(db.query).toHaveBeenCalledWith(
        expect.stringContaining('SELECT'),
        ['user-123']
      );
    });
  });

  describe('PATCH /api/notifications/:notificationId/read', () => {
    it('should mark notification as read and return 204', async () => {
      (db.query as jest.Mock).mockResolvedValue([{ affectedRows: 1 }]);

      const response = await request(app).patch('/api/notifications/noti-1/read');

      expect(response.status).toBe(204);
      expect(db.query).toHaveBeenCalledWith(
        expect.stringContaining('UPDATE'),
        ['noti-1']
      );
    });

    it('should return 404 if notification does not exist', async () => {
      (db.query as jest.Mock).mockResolvedValue([{ affectedRows: 0 }]);

      const response = await request(app).patch('/api/notifications/nonexistent/read');

      expect(response.status).toBe(404);
    });
  });
});
