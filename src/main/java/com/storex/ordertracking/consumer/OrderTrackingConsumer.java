package com.storex.ordertracking.consumer;

import com.storex.ordertracking.model.OrderTrackingEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderTrackingConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderTrackingConsumer.class);

    @KafkaListener(
            topics = "${storex.kafka.order-tracking-topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, OrderTrackingEvent> record) {
        OrderTrackingEvent event = record.value();

        log.info("Consume order event: key={}, orderId={}, status={}, partition={}, offset={}",
                record.key(),
                event.orderId(),
                event.status(),
                record.partition(),
                record.offset());

        // Xu ly cap nhat trang thai don hang tai day.
    }
}

