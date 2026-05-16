package com.example.surge.producer;


import com.example.surge.model.RideRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class RideRequestProducer {

    private final KafkaTemplate<String, RideRequest> kafkaTemplate;

    public RideRequestProducer(KafkaTemplate<String, RideRequest> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendRideRequest(RideRequest request) {

        kafkaTemplate.send(
                "ride-requests",
                request.getZoneId(),
                request
        );
    }
}