package com.storex.ordertracking.controller;

import com.storex.ordertracking.model.OrderStatus;
import com.storex.ordertracking.producer.OrderTrackingProducer;
import java.util.Map;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/order-tracking")
public class OrderTrackingController {

    private final OrderTrackingProducer producer;

    public OrderTrackingController(OrderTrackingProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/{orderId}/demo-flow")
    public Map<String, String> publishDemoFlow(@PathVariable String orderId) {
        producer.publishDemoFlow(orderId);
        return Map.of("message", "Published CREATED -> PAID -> SHIPPED for order " + orderId);
    }

    @PostMapping("/{orderId}/{status}")
    public Map<String, String> publishStatus(@PathVariable String orderId, @PathVariable OrderStatus status) {
        producer.publishStatusChanged(orderId, status);
        return Map.of("message", "Published " + status + " for order " + orderId);
    }
}

