package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private long eventTime;
    private String eventType;
    private String productId;
    private String categoryId;
    private String categoryCode;
    private String brand;
    private double price;
    private String userid;
    private String userSession;
}

