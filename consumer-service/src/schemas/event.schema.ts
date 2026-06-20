import { z } from 'zod';

export const KafkaEventSchema = z.object({
  event_id: z.string().min(1, 'event_id is required'),
  timestamp: z.string().min(1, 'timestamp is required'),
  source: z.string().min(1, 'source is required'),
  event_type: z.enum(['user_liked_post', 'user_commented']),
  payload: z.object({
    user_id: z.string().min(1, 'user_id is required'),
    target_id: z.string().min(1, 'target_id is required'),
    recipient_id: z.string().min(1, 'recipient_id is required'),
    comment_text: z.string().optional(),
  }),
});

export type KafkaEvent = z.infer<typeof KafkaEventSchema>;
