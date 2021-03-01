package com.grpc.kafka.example.protocrud.listener;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import com.grpc.kafka.example.protocrud.LogType;
import com.grpc.kafka.example.protocrud.Notification;
import com.grpc.kafka.example.protocrud.configuration.KafkaConsumerConfiguration;
import com.grpc.kafka.example.protocrud.serialization.NotificationSerializer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(topics = { "logging" }, partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092",
        "port=9092" })
@ContextConfiguration(classes = {KafkaConsumerConfiguration.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NotificationListenerIT {
    
    @SpyBean
    private NotificationListener notificationListenerMock;

    @Autowired
    private EmbeddedKafkaBroker kafkaEmbedded;

    Producer<String, Notification> producer;

    @Value("${topic}")
    private String TOPIC;

    @BeforeAll
    public void setup(){
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(kafkaEmbedded));
        producer = new DefaultKafkaProducerFactory<String, Notification>(configs, new StringSerializer(), new NotificationSerializer()).createProducer();   
    }

    @AfterAll
    public void teardown(){
        if(producer != null){
            producer.close();
        }
    }

    @Test
    public void kafkaListenerConfiguredCorrectlyToFilterForErrors(){
        Notification notificationError = Notification.newBuilder()
                                                .setService("some service name")
                                                .setClass_("some class name")
                                                .setMessage("this is a test")
                                                .setType(LogType.ERROR)
                                                .build();
                                        
        producer.send(new ProducerRecord<String, Notification>(TOPIC, notificationError));
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            fail("InterruptException thrown. " + e.getMessage());
        }
  
        verify(notificationListenerMock).logNotification(notificationError);
    }
}
