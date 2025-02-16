package com.colak.springtutorial.consumer;

import com.colak.springtutorial.pojo.BrokerMessage;
import com.colak.springtutorial.pojo.BrokerMessageStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageQueueConsumer {

    private final JdbcClient jdbcClient;

    private final TransactionTemplate transactionTemplate;

    // Process 10 messages at a time
    private static final int BATCH_SIZE = 10;

    public void consumeMessages() {
        transactionTemplate.execute(_ -> {
            List<BrokerMessage> brokerMessages = fetchMessages();
            processAndUpdateMessages(brokerMessages);
            return null;
        });
    }

    private List<BrokerMessage> fetchMessages() {
        // Fetch messages with 'PENDING' status in batches
        // Order by id to process in insertion order
        String selectQuery = """
                SELECT TOP %d * FROM MessageQueue WITH (UPDLOCK)\s
                WHERE status = :sts\s
                ORDER BY id ASC"""
                .formatted(BATCH_SIZE);

        return jdbcClient
                .sql(selectQuery)
                .param("sts", BrokerMessageStatus.PENDING.name())
                .query((rs, rowNum) -> {
                    BrokerMessage brokerMessage = new BrokerMessage();
                    brokerMessage.setId(rs.getInt("id"));
                    brokerMessage.setMessageContent(rs.getString("message"));
                    brokerMessage.setStatus(BrokerMessageStatus.valueOf(rs.getString("status")));

                    // Convert from Timestamp to LocalDateTime
                    brokerMessage.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());

                    // Convert from Timestamp to LocalDateTime
                    Timestamp processedAt = rs.getTimestamp("processed_at");
                    brokerMessage.setProcessedAt(processedAt != null ? processedAt.toLocalDateTime() : null);
                    return brokerMessage;
                }).
                list();
    }

    private void processAndUpdateMessages(List<BrokerMessage> brokerMessageList) {
        if (!brokerMessageList.isEmpty()) {

            List<Long> messageIds = new ArrayList<>();

            // Loop through each message and update individually
            for (BrokerMessage brokerMessage : brokerMessageList) {
                // Process messages
                try {
                    processMessage(brokerMessage);
                    messageIds.add(brokerMessage.getId());
                } catch (Exception exception) {
                    log.info("Error processing message with ID : {}", brokerMessage.getId());
                }
            }
            updateMessages(messageIds);

        } else {
            System.out.println("No messages to process.");
        }
    }

    private void processMessage(BrokerMessage brokerMessage) {
        System.out.println("Processed brokerMessage with ID: " + brokerMessage.getId());
    }

    private void updateMessages(List<Long> messageIds) {
        // Update their status to PROCESSED
        // Create the parameterized update query with IN clause
        String updateQuery = "UPDATE MessageQueue SET status = :sts, processed_at = GETDATE() WHERE id IN (:ids)";

        jdbcClient.sql(updateQuery)
                .param("sts", BrokerMessageStatus.PROCESSED.name())
                .param("ids", messageIds)
                .update();
    }
}

