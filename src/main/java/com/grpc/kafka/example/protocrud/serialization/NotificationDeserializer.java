package com.grpc.kafka.example.protocrud.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.grpc.kafka.example.protocrud.Notification;
import com.grpc.kafka.example.protocrud.Notification.Builder;

import org.apache.kafka.common.serialization.Deserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NotificationDeserializer implements Deserializer<Notification> {

    @Override
    public Notification deserialize(final String topic, byte[] data) {
        String jsonString = new String(data);
        Builder notification = Notification.newBuilder();
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(jsonString, notification);
            return notification.build();
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot parse this message. " + e.getMessage(), e);
        }
        
        return null;
    }
    
}
