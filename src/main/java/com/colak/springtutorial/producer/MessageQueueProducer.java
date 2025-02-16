package com.colak.springtutorial.producer;

import com.colak.springtutorial.pojo.BrokerMessage;
import com.colak.springtutorial.pojo.BrokerMessageStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MessageQueueProducer {

    private final JdbcClient jdbcClient;

    // Insert a brokerMessage into the MessageQueue
    public void produceMessage(BrokerMessage brokerMessage) {
        String insertQuery = "INSERT INTO MessageQueue (message, status, created_at) VALUES (:brokerMessage, :sts, GETDATE())";


        // Execute insert query using JdbcClient
        jdbcClient
                .sql(insertQuery)
                .param("brokerMessage", brokerMessage.getMessageContent())
                .param("sts", BrokerMessageStatus.PENDING.name())

                .update();

        System.out.println("BrokerMessage produced: " + brokerMessage.getMessageContent());
    }
}

