package com.grpc.kafka.example.protocrud.listener;

import com.grpc.kafka.example.protocrud.Notification;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {
    
    @KafkaListener(topics = "${topic}", groupId = "${groupId}", containerFactory = "kafkaListenerContainerFactory")
    public void logNotification (@Payload Notification notification){
        System.out.println("***********************************");
        System.out.println(buildLogOutput(notification));
        System.out.println("***********************************");
    }

    String buildLogOutput (Notification notification){
        return String.format("%s:%s:%s - %s", 
            notification.getService(), 
            notification.getClass_(), 
            notification.getType(), 
            notification.getMessage()
        );
    }
}
