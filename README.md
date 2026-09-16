# SS10 HW05: Đảm bảo thứ tự sự kiện với Kafka Partition Key

**Sinh viên:** Dang Khanh An  
**Lớp:** IT214  
**Mã:** PTIT070

## 1. Bối cảnh

Hệ thống StoreX phát sinh các sự kiện trạng thái đơn hàng vào topic `order-tracking`:

```text
CREATED -> PAID -> SHIPPED
```

Ràng buộc nghiệp vụ là các sự kiện của cùng một `orderId` phải được xử lý đúng thứ tự. Ví dụ đơn `ORD-1001` không được xử lý `SHIPPED` trước `PAID`.

## 2. Vì sao partition có thể làm mất thứ tự

Kafka chỉ đảm bảo thứ tự message **bên trong cùng một partition**. Kafka không đảm bảo thứ tự tuyệt đối giữa các partition khác nhau.

Ví dụ topic `order-tracking` có 5 partition. Nếu producer gửi:

```text
ORD-1001 - CREATED -> partition 0
ORD-1001 - PAID    -> partition 3
ORD-1001 - SHIPPED -> partition 1
```

thì ba consumer khác nhau có thể xử lý ba partition này song song. Khi đó `SHIPPED` có thể được xử lý trước `PAID`, gây lỗi nghiệp vụ.

Vì vậy vấn đề không nằm ở Kafka, mà nằm ở cách producer chọn key khi gửi message.

## 3. Cách khắc phục bằng Partition Key

Khi gửi message, producer phải dùng `orderId` làm key:

```java
kafkaTemplate.send("order-tracking", orderId, event);
```

Kafka mặc định tính hash của key để chọn partition:

```text
partition = hash(orderId) % numberOfPartitions
```

Với cùng một `orderId`, kết quả hash giống nhau, nên tất cả event của cùng đơn hàng luôn đi vào cùng một partition cố định. Khi đã nằm cùng partition, Kafka giữ thứ tự ghi và consumer đọc theo đúng thứ tự offset.

## 4. Code Producer

File chính:

```text
src/main/java/com/storex/ordertracking/producer/OrderTrackingProducer.java
```

Đoạn quan trọng:

```java
public void publishStatusChanged(String orderId, OrderStatus status) {
    OrderTrackingEvent event = OrderTrackingEvent.of(orderId, status);
    kafkaTemplate.send(orderTrackingTopic, orderId, event);
}
```

Ở đây `orderId` là key. Không dùng random key, không để key null.

## 5. Code Consumer

File chính:

```text
src/main/java/com/storex/ordertracking/consumer/OrderTrackingConsumer.java
```

Consumer nhận `ConsumerRecord` để log rõ key, partition và offset:

```java
@KafkaListener(topics = "${storex.kafka.order-tracking-topic}",
        groupId = "${spring.kafka.consumer.group-id}")
public void consume(ConsumerRecord<String, OrderTrackingEvent> record) {
    OrderTrackingEvent event = record.value();
    log.info("Consume order event: key={}, orderId={}, status={}, partition={}, offset={}",
            record.key(), event.orderId(), event.status(), record.partition(), record.offset());
}
```

Khi chạy thử với một `orderId`, log sẽ cho thấy các event `CREATED`, `PAID`, `SHIPPED` có cùng key và cùng partition.

## 6. Số lượng Consumer tối đa

Topic có 5 partition, nên trong cùng một consumer group, số consumer hữu ích tối đa là:

```text
5 consumer
```

Lý do: một partition trong một consumer group chỉ được gán cho một consumer tại một thời điểm. Nếu bật 6 consumer cho topic có 5 partition, consumer thứ 6 sẽ không được gán partition nào và bị nhàn rỗi. Vì vậy bật quá số partition không giúp tăng tốc, chỉ lãng phí tài nguyên.

Trong bài này có thể cấu hình:

```yaml
spring:
  kafka:
    listener:
      concurrency: 5
```

## 7. Cấu hình đáng chú ý

```yaml
spring:
  kafka:
    producer:
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 1
```

`enable.idempotence=true` giúp producer giảm rủi ro gửi trùng khi retry. `max.in.flight.requests.per.connection=1` giúp hạn chế nguy cơ đảo thứ tự khi producer retry trong một số tình huống lỗi mạng.

## 8. Chạy kiểm tra

Build project:

```bash
./gradlew build
```

Khi chạy thật cần Kafka tại `localhost:9092` và topic:

```text
order-tracking
```

Gửi thử chuỗi sự kiện demo:

```bash
curl -X POST http://localhost:8083/api/order-tracking/ORD-1001/demo-flow
```

Kỳ vọng log producer/consumer thể hiện cả ba event của `ORD-1001` đi vào cùng một partition và được đọc theo thứ tự:

```text
CREATED -> PAID -> SHIPPED
```

