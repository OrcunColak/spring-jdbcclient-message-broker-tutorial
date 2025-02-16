package com.colak.springtutorial.consumer;

import com.colak.springtutorial.pojo.BrokerMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;

@Service
@RequiredArgsConstructor
public class MessageQueueConsumer {

    private final JdbcClient jdbcClient;

    private static final int BATCH_SIZE = 10;  // Process 10 messages at a time

    // Poll every 10 seconds for unprocessed messages
    @Scheduled(fixedRate = 10000)
    public void consumeMessages() {
        String selectQuery = "SELECT TOP " + BATCH_SIZE + " * FROM MessageQueue WHERE status = 'PENDING' ORDER BY id ASC";

        // Fetch messages with 'PENDING' status in batches
        List<BrokerMessage> brokerMessageList = jdbcClient
                .sql(selectQuery)
                .query((rs, rowNum) -> {
                    BrokerMessage brokerMessage = new BrokerMessage();
                    brokerMessage.setId(rs.getInt("id"));
                    brokerMessage.setMessageContent(rs.getString("brokerMessage"));
                    brokerMessage.setStatus(rs.getString("status"));
                    brokerMessage.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                    Timestamp processedAt = rs.getTimestamp("processed_at");
                    brokerMessage.setProcessedAt(processedAt != null ? processedAt.toLocalDateTime() : null);
                    return brokerMessage;
                }).
                list();


        if (!brokerMessageList.isEmpty()) {
            // Process messages and update their status to PROCESSED
            String updateQuery = "UPDATE MessageQueue SET status = 'PROCESSED', processed_at = GETDATE() WHERE id = ?";

            // Loop through each message and update individually
            for (BrokerMessage brokerMessage : brokerMessageList) {
                MapSqlParameterSource params = new MapSqlParameterSource()
                        .addValue("id", brokerMessage.getId());

                jdbcClient.sql(updateQuery)
                        .param(params)
                        .update();

                System.out.println("Processed brokerMessage with ID: " + brokerMessage.getId());
            }
        } else {
            System.out.println("No messages to process.");
        }
    }
}

