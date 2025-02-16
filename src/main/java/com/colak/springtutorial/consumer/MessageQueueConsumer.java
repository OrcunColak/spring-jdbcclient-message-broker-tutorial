package com.colak.springtutorial.consumer;

import com.colak.springtutorial.pojo.BrokerMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
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

    // Poll every 10 seconds for unprocessed messages
    @Scheduled(fixedRate = 10_000)
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
                WHERE status = 'PENDING'\s
                ORDER BY id ASC"""
                .formatted(BATCH_SIZE);

        return jdbcClient
                .sql(selectQuery)
                .query((rs, rowNum) -> {
                    BrokerMessage brokerMessage = new BrokerMessage();
                    brokerMessage.setId(rs.getInt("id"));
                    brokerMessage.setMessageContent(rs.getString("brokerMessage"));
                    brokerMessage.setStatus(rs.getString("status"));

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
        String updateQuery = "UPDATE MessageQueue SET status = 'PROCESSED', processed_at = GETDATE() WHERE id IN (:ids)";
        // Perform the update in a single batch using the list of IDs
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("ids", messageIds);

        jdbcClient.sql(updateQuery)
                .param(params)
                .update();
    }
}

