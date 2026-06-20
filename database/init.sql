CREATE DATABASE IF NOT EXISTS notification_db;
USE notification_db;

CREATE TABLE IF NOT EXISTS notifications (
    notification_id VARCHAR(36) PRIMARY KEY,
    recipient_user_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    message_content TEXT NOT NULL,
    status ENUM('unread', 'read') DEFAULT 'unread',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_event_id VARCHAR(36) UNIQUE NOT NULL
);

-- Index for retrieving unread notifications quickly
CREATE INDEX idx_recipient_user_id_status ON notifications (recipient_user_id, status);
