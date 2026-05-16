package com.example.surge.model;

import lombok.Data;

@Data
public class RideRequest {

    private String userId;

    private String zoneId;

    private String timestamp;

}