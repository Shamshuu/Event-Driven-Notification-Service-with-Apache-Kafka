import { v4 as uuidv4 } from 'uuid';
import { db } from '../config/db';
import { logger } from '../config/logger';
import { KafkaEvent, KafkaEventSchema } from '../schemas/event.schema';

export async function processNotificationEvent(rawMessage: string): Promise<void> {
  let event: KafkaEvent;

  // 1. Parse and validate message JSON
  try {
    const parsed = JSON.parse(rawMessage);
    const parseResult = KafkaEventSchema.safeParse(parsed);
    if (!parseResult.success) {
      logger.error(
        `Malformed event received. Validation failed: ${JSON.stringify(
          parseResult.error.format()
        )}`
      );
      // Exit early for malformed messages so we don't clog the topic
      return;
    }
    event = parseResult.data;
  } catch (err) {
    logger.error('Failed to parse Kafka message JSON:', err);
    return;
  }

  const { event_id, event_type, payload } = event;

  // 2. Format message content based on type
  let messageContent = '';
  if (event_type === 'user_liked_post') {
    messageContent = `Your post was liked by ${payload.user_id}.`;
  } else if (event_type === 'user_commented') {
    const commentText = payload.comment_text || '';
    messageContent = `Your post received a comment from ${payload.user_id}: ${commentText}`;
  }

  const notificationId = uuidv4();

  // 3. Persist to database with idempotency check
  try {
    logger.info(
      `Processing event ${event_id} (${event_type}) for recipient ${payload.recipient_id}`
    );

    await db.query(
      `INSERT INTO notifications (notification_id, recipient_user_id, event_type, message_content, status, processed_event_id)
       VALUES (?, ?, ?, ?, 'unread', ?)`,
      [notificationId, payload.recipient_id, event_type, messageContent, event_id]
    );

    logger.info(
      `Successfully processed event ${event_id}. Notification stored: ${notificationId}`
    );
  } catch (error: any) {
    // ER_DUP_ENTRY is 1062: Duplicate entry for KEY 'processed_event_id'
    if (error.code === 'ER_DUP_ENTRY' || error.errno === 1062) {
      logger.warn(
        `Idempotency block: Event ${event_id} has already been processed. Skipping database write.`
      );
      return; // Return without throwing so the Kafka consumer commits the offset
    }

    logger.error(`Database error processing event ${event_id}:`, error);
    throw error; // Throw so that KafkaJS retries
  }
}
