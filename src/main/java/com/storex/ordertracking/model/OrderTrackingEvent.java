package com.storex.ordertracking.model;

import java.time.Instant;

public record OrderTrackingEvent(
        String orderId,
        OrderStatus status,
        Instant occurredAt
) {
    public static OrderTrackingEvent of(String orderId, OrderStatus status) {
        return new OrderTrackingEvent(orderId, status, Instant.now());
    }
}

