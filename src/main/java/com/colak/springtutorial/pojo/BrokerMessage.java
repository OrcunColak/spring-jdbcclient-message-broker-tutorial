package com.colak.springtutorial.pojo;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;


@Getter
@Setter
public class BrokerMessage {
    private long id;

    private String messageContent;

    private BrokerMessageStatus status;

    private LocalDateTime createdAt;

    private LocalDateTime processedAt;
}

