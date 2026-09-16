package com.storex.ordertracking.producer;

import com.storex.ordertracking.model.OrderStatus;
import com.storex.ordertracking.model.OrderTrackingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderTrackingProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderTrackingProducer.class);

    private final KafkaTemplate<String, OrderTrackingEvent> kafkaTemplate;
    private final String orderTrackingTopic;

    public OrderTrackingProducer(
            KafkaTemplate<String, OrderTrackingEvent> kafkaTemplate,
            @Value("${storex.kafka.order-tracking-topic}") String orderTrackingTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderTrackingTopic = orderTrackingTopic;
    }

    public void publishStatusChanged(String orderId, OrderStatus status) {
        OrderTrackingEvent event = OrderTrackingEvent.of(orderId, status);

        kafkaTemplate.send(orderTrackingTopic, orderId, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Publish order event failed: orderId={}, status={}, error={}",
                                orderId, status, ex.getMessage(), ex);
                        return;
                    }

                    log.info("Published order event: orderId={}, status={}, partition={}, offset={}",
                            orderId,
                            status,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                });
    }

    public void publishDemoFlow(String orderId) {
        publishStatusChanged(orderId, OrderStatus.CREATED);
        publishStatusChanged(orderId, OrderStatus.PAID);
        publishStatusChanged(orderId, OrderStatus.SHIPPED);
    }
}

