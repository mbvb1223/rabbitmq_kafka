# Demo 05: conclusions / kết luận

[English](#english) · [Tiếng Việt](#tiếng-việt)

## English

**One sentence:** in RabbitMQ, TTL and priority are properties **of each message** and the broker enforces them. In Kafka, "TTL" is topic retention: disk management that deletes whole segments on a timer and tells no one. Priority is something you build out of topics and your own loop.

| | RabbitMQ | Kafka |
|---|---|---|
| TTL set on | each message (`expiration`) or the queue (`x-message-ttl`); the shorter wins | the topic (`retention.ms`) |
| What expires | one message, once it reaches the head | a whole segment, once its **newest** record is older than `retention.ms` |
| When | as soon as the expired message is at the head | at the next retention check (every 5 min) |
| After expiry | dead-lettered with `x-death` reason `expired` if a DLX is set, otherwise dropped | deleted; nobody is told |
| Priority | 0-31 per message, strict, in one queue (4.3 quorum) | one topic per level + a worker that checks high first |
| What breaks priority | a large prefetch | one consumer on both topics (librdkafka prefetch) |

### Remember

- **A per-message TTL can only shorten the queue's:** `expiration` 20000 on a 10 s queue still expires after 10 s.
- **Expiry happens at the head only:** 8000 then 2000 → both dead-lettered at 8 s. The queue depth still counts expired messages stuck behind the head. Don't mix TTLs in one queue.
- **No DLX = expired messages are dropped silently.** The DLX is opt-in.
- **4.3 quorum queues have 32 strict levels with no queue argument.** Above 31 counts as 31, so p31, p50, p255 came out in publish order. A message with no priority counts as 4.
- **Prefetch = how many unacked messages the broker may hand one consumer.** Priority only reorders what is still in the queue: prefetch 1 → p31 ran ~3rd, prefetch 10 → 11th. If every message is queued before the consumer starts, prefetch doesn't change the order.
- **Kafka retention isn't TTL:** a segment is deleted only when its newest record is older than `retention.ms`, checked every 5 min. A steady trickle keeps old records alive for up to `segment.ms` (7 days). Retention also deletes **unprocessed** messages, with no DLQ.
- **Kafka priority = two topics + two consumers** (each with its own fetch buffer) + a loop that checks high before every low. Strict on both brokers: a steady high stream starves low.
- **TTL is a business rule, retention is disk management.** A per-message TTL in Kafka is your consumer comparing the record timestamp and skipping stale records.

## Tiếng Việt

**Một câu:** ở RabbitMQ, TTL và priority là thuộc tính **của từng message**, và broker tự thực thi. Ở Kafka, "TTL" thực chất là retention của topic: cơ chế quản lý dung lượng đĩa, xoá cả segment theo chu kỳ và không báo cho ai. Priority thì bạn phải tự xây bằng nhiều topic cộng với vòng lặp consumer của mình.

| | RabbitMQ | Kafka |
|---|---|---|
| TTL đặt ở đâu | từng message (`expiration`) hoặc queue (`x-message-ttl`); giá trị ngắn hơn thắng | topic (`retention.ms`) |
| Cái gì hết hạn | 1 message, khi nó lên đầu queue | cả segment, khi record **mới nhất** trong đó cũ hơn `retention.ms` |
| Khi nào | ngay khi message hết hạn nằm ở đầu queue | ở lần retention check tiếp theo (5 phút một lần) |
| Sau khi hết hạn | có DLX thì bị dead-letter với `x-death` reason `expired`, không có DLX thì bị bỏ | bị xoá, không ai được báo |
| Priority | 0-31 trên từng message, strict, trong 1 queue (quorum 4.3) | mỗi mức 1 topic + worker kiểm tra high trước |
| Cái gì làm hỏng priority | prefetch lớn | 1 consumer subscribe cả 2 topic (librdkafka prefetch) |

### Ghi nhớ

- **TTL của message chỉ rút ngắn được TTL của queue:** `expiration` 20000 trên queue 10 s vẫn hết hạn sau 10 s.
- **Message chỉ hết hạn khi nằm ở đầu queue:** publish 8000 rồi 2000 thì cả hai bị dead-letter lúc 8 s. Message đã hết hạn nhưng kẹt phía sau vẫn được tính vào queue depth. Đừng trộn nhiều TTL trong cùng 1 queue.
- **Không có DLX thì message hết hạn bị bỏ âm thầm.** DLX phải tự cấu hình.
- **Quorum queue 4.3 có 32 mức strict, không cần queue argument.** Trên 31 tính là 31, nên p31, p50, p255 ra theo thứ tự publish. Message không đặt priority thì tính là 4.
- **Prefetch = số message chưa ack tối đa mà broker được giao cho 1 consumer.** Priority chỉ sắp xếp lại message **còn trong queue**: prefetch 1 → p31 chạy thứ ~3, prefetch 10 → thứ 11. Nếu mọi message đã có sẵn trong queue trước khi consumer chạy thì prefetch không đổi thứ tự.
- **Retention của Kafka không phải TTL:** segment chỉ bị xoá khi record mới nhất cũ hơn `retention.ms`, và việc này chỉ được kiểm tra 5 phút một lần. Nếu topic liên tục có message mới thì record cũ có thể còn đến `segment.ms` (7 ngày). Retention cũng xoá cả message **chưa xử lý**, và không có DLQ.
- **Priority trong Kafka = 2 topic + 2 consumer** (mỗi consumer có buffer fetch riêng) + vòng lặp kiểm tra high trước mỗi job low. Cả hai broker đều strict: high đổ về liên tục thì low bị bỏ đói.
- **TTL là quy tắc nghiệp vụ, retention là quản lý dung lượng đĩa.** Muốn có TTL theo từng message trong Kafka thì consumer phải tự so timestamp của record và bỏ qua record quá cũ.
