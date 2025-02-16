package com.colak.springtutorial.producer;

import com.colak.springtutorial.pojo.BrokerMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MessageQueueProducer {

    private final JdbcClient jdbcClient;

    // Insert a brokerMessage into the MessageQueue
    public void produceMessage(BrokerMessage brokerMessage) {
        String insertQuery = "INSERT INTO MessageQueue (brokerMessage, status, created_at) VALUES (:brokerMessage, 'PENDING', GETDATE())";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("brokerMessage", brokerMessage.getMessageContent());

        // Execute insert query using JdbcClient
        jdbcClient
                .sql(insertQuery)
                .params(params)
                .update();

        System.out.println("BrokerMessage produced: " + brokerMessage.getMessageContent());
    }
}

