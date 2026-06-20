import { processNotificationEvent } from '../src/services/notification.service';
import { db } from '../src/config/db';

// Mock DB configuration for isolation
jest.mock('../src/config/db', () => ({
  db: {
    query: jest.fn(),
  },
  checkDbConnection: jest.fn().mockResolvedValue(true),
}));

describe('Consumer Service processing', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should process user_liked_post event and insert notification', async () => {
    const event = {
      event_id: 'event-liked-123',
      timestamp: new Date().toISOString(),
      source: 'api-service',
      event_type: 'user_liked_post',
      payload: {
        user_id: 'actor-123',
        target_id: 'post-456',
        recipient_id: 'recipient-789',
      },
    };

    (db.query as jest.Mock).mockResolvedValue([{ affectedRows: 1 }]);

    await processNotificationEvent(JSON.stringify(event));

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(db.query).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO notifications'),
      [
        expect.any(String), // notificationId
        'recipient-789',
        'user_liked_post',
        'Your post was liked by actor-123.',
        'event-liked-123',
      ]
    );
  });

  it('should process user_commented event and insert notification', async () => {
    const event = {
      event_id: 'event-comment-123',
      timestamp: new Date().toISOString(),
      source: 'api-service',
      event_type: 'user_commented',
      payload: {
        user_id: 'actor-123',
        target_id: 'post-456',
        recipient_id: 'recipient-789',
        comment_text: 'Hello world',
      },
    };

    (db.query as jest.Mock).mockResolvedValue([{ affectedRows: 1 }]);

    await processNotificationEvent(JSON.stringify(event));

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(db.query).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO notifications'),
      [
        expect.any(String), // notificationId
        'recipient-789',
        'user_commented',
        'Your post received a comment from actor-123: Hello world',
        'event-comment-123',
      ]
    );
  });

  it('should skip duplicate events gracefully and not throw error', async () => {
    const event = {
      event_id: 'event-duplicate-123',
      timestamp: new Date().toISOString(),
      source: 'api-service',
      event_type: 'user_liked_post',
      payload: {
        user_id: 'actor-123',
        target_id: 'post-456',
        recipient_id: 'recipient-789',
      },
    };

    const duplicateError = new Error('Duplicate entry') as any;
    duplicateError.code = 'ER_DUP_ENTRY';
    duplicateError.errno = 1062;

    (db.query as jest.Mock).mockRejectedValue(duplicateError);

    // This should NOT throw because ER_DUP_ENTRY represents an expected idempotency check block
    await expect(processNotificationEvent(JSON.stringify(event))).resolves.not.toThrow();
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it('should throw an error on generic database failures to trigger Kafka retry', async () => {
    const event = {
      event_id: 'event-fail-123',
      timestamp: new Date().toISOString(),
      source: 'api-service',
      event_type: 'user_liked_post',
      payload: {
        user_id: 'actor-123',
        target_id: 'post-456',
        recipient_id: 'recipient-789',
      },
    };

    const connError = new Error('Database connection failed');
    (db.query as jest.Mock).mockRejectedValue(connError);

    // This SHOULD throw to trigger Kafka consumption retry
    await expect(processNotificationEvent(JSON.stringify(event))).rejects.toThrow(
      'Database connection failed'
    );
  });

  it('should ignore malformed events and not write to database', async () => {
    const invalidJson = '{ malformed JSON }';
    await processNotificationEvent(invalidJson);
    expect(db.query).not.toHaveBeenCalled();

    const missingFieldsEvent = {
      event_id: 'event-missing-123',
      // event_type is missing
      payload: {
        user_id: 'actor-123',
      },
    };
    await processNotificationEvent(JSON.stringify(missingFieldsEvent));
    expect(db.query).not.toHaveBeenCalled();
  });
});
