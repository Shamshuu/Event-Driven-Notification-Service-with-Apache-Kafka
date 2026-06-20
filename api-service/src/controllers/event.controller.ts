import { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { PublishEventSchema } from '../schemas/event.schema';
import { producer } from '../config/kafka';
import { logger } from '../config/logger';

const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'user-activity';

export async function publishUserActivityEvent(req: Request, res: Response): Promise<void> {
  try {
    // 1. Validate request body
    const parseResult = PublishEventSchema.safeParse(req.body);
    if (!parseResult.success) {
      logger.warn(`Invalid publish event payload: ${JSON.stringify(parseResult.error.format())}`);
      res.status(400).json({
        error: 'Bad Request',
        message: 'Invalid request body',
        details: parseResult.error.format(),
      });
      return;
    }

    const { event_type, payload } = parseResult.data;

    // 2. Generate unique event_id and metadata
    const eventId = uuidv4();
    const timestamp = new Date().toISOString();

    // 3. Normalize payload fields for Kafka message
    // Map liked_by_user_id or recipient_id to recipient_id, post_id to target_id
    const recipientId = payload.recipient_id || payload.liked_by_user_id;
    const targetId = payload.post_id;

    const kafkaPayload: Record<string, any> = {
      user_id: payload.user_id,
      target_id: targetId,
      recipient_id: recipientId,
    };

    if (event_type === 'user_commented' && 'comment_text' in payload) {
      kafkaPayload.comment_text = payload.comment_text;
    }

    const kafkaMessage = {
      event_id: eventId,
      timestamp,
      source: 'api-service',
      event_type,
      payload: kafkaPayload,
    };

    // 4. Publish message to Kafka
    logger.info(`Publishing event to Kafka topic '${KAFKA_TOPIC}'. Event type: ${event_type}, ID: ${eventId}`);
    
    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [
        {
          key: recipientId, // Route events for same recipient to same partition to maintain order
          value: JSON.stringify(kafkaMessage),
        },
      ],
    });

    logger.info(`Event successfully published to Kafka. Event ID: ${eventId}`);

    res.status(202).json({
      message: 'Event published successfully',
      event_id: eventId,
    });
  } catch (error) {
    logger.error('Error publishing event to Kafka:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to publish event to message broker',
    });
  }
}
