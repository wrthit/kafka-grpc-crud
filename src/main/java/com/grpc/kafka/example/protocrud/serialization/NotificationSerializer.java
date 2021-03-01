package com.grpc.kafka.example.protocrud.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.grpc.kafka.example.protocrud.Notification;

import org.apache.kafka.common.serialization.Serializer;

public class NotificationSerializer implements Serializer<Notification> {

    @Override
    public byte[] serialize(final String topic, final Notification data) {
        String jsonString = "";
        try {
            jsonString = JsonFormat.printer().print(data);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return jsonString.getBytes();
    }
}
