package com.example.surge.simulator;

import com.example.surge.model.RideRequest;
import com.example.surge.producer.RideRequestProducer;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;

@Component
public class RideRequestSimulator {

    private final RideRequestProducer producer;

    private final Random random = new Random();

    private final List<String> zones = List.of(
            "Delhi-CP",
            "Gurgaon-Sector-29",
            "Noida-62"
    );

    public RideRequestSimulator(RideRequestProducer producer) {
        this.producer = producer;
    }

    @PostConstruct
    public void startSimulation() {

        new Thread(() -> {

            while (true) {

                try {

                    RideRequest request = new RideRequest();

                    request.setUserId(
                            String.valueOf(random.nextInt(1000))
                    );

                    request.setZoneId(
                            zones.get(random.nextInt(zones.size()))
                    );

                    request.setTimestamp(
                            LocalDateTime.now().toString()
                    );

                    producer.sendRideRequest(request);

                    System.out.println(
                            "SENT REQUEST → " + request
                    );

                    Thread.sleep(1000);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }).start();
    }
}