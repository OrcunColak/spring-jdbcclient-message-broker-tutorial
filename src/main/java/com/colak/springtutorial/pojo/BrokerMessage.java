package com.colak.springtutorial.pojo;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Setter
@Getter
public class BrokerMessage {
    private long id;
    private String messageContent;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime processedAt;

}

