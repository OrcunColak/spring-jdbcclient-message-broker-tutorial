package com.colak.springtutorial.scheduler;

import com.colak.springtutorial.consumer.MessageQueueConsumer;
import com.colak.springtutorial.pojo.BrokerMessage;
import com.colak.springtutorial.producer.MessageQueueProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class Scheduler {

    private final MessageQueueProducer messageQueueProducer;
    private final MessageQueueConsumer messageQueueConsumer;

    private int counter;

    @Scheduled(fixedRate = 10_000)
    public void produceMessages() {
        BrokerMessage brokerMessage = new BrokerMessage();
        brokerMessage.setMessageContent(String.valueOf(counter++));

        messageQueueProducer.produceMessage(brokerMessage);
    }

    @Scheduled(fixedRate = 15_000)
    public void consumeMessages() {
        messageQueueConsumer.consumeMessages();
    }


}
