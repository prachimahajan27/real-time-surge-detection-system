package com.example.surge.controller;



import com.example.surge.model.RideRequest;

import com.example.surge.producer.RideRequestProducer;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/rides")
public class RideRequestController {

    private final RideRequestProducer producer;

    public RideRequestController(RideRequestProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/request")
    public String requestRide(@RequestBody RideRequest request) {

        producer.sendRideRequest(request);

        return "Ride request sent successfully!";
    }
}