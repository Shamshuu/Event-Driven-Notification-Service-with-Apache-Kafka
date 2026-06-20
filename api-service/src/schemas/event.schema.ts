import { z } from 'zod';

export const UserLikedPostPayloadSchema = z
  .object({
    user_id: z.string().min(1, 'user_id is required'),
    post_id: z.string().min(1, 'post_id is required'),
    recipient_id: z.string().min(1).optional(),
    liked_by_user_id: z.string().min(1).optional(),
  })
  .refine((data) => data.recipient_id || data.liked_by_user_id, {
    message: 'Either recipient_id or liked_by_user_id must be provided in the payload',
    path: ['recipient_id'],
  });

export const UserCommentedPayloadSchema = z
  .object({
    user_id: z.string().min(1, 'user_id is required'),
    post_id: z.string().min(1, 'post_id is required'),
    comment_text: z.string().min(1, 'comment_text is required'),
    recipient_id: z.string().min(1).optional(),
    liked_by_user_id: z.string().min(1).optional(),
  })
  .refine((data) => data.recipient_id || data.liked_by_user_id, {
    message: 'Either recipient_id or liked_by_user_id must be provided in the payload',
    path: ['recipient_id'],
  });

export const PublishEventSchema = z.discriminatedUnion('event_type', [
  z.object({
    event_type: z.literal('user_liked_post'),
    payload: UserLikedPostPayloadSchema,
  }),
  z.object({
    event_type: z.literal('user_commented'),
    payload: UserCommentedPayloadSchema,
  }),
]);

export const GetNotificationsSchema = z.object({
  userId: z.string().min(1, 'userId path parameter is required'),
});

export const MarkReadSchema = z.object({
  notificationId: z.string().min(1, 'notificationId path parameter is required'),
});
