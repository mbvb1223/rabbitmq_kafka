# Demo 04: conclusions / kết luận

[English](#english) · [Tiếng Việt](#tiếng-việt)

## English

**One sentence:** in RabbitMQ the **consumer** decides what it gets (it binds a pattern on the routing key's words and the broker filters); in Kafka the **producer's topic layout** decides, because the broker never filters on content: a consumer reads everything and filters in PHP, or someone adds topics.

| | RabbitMQ | Kafka |
|---|---|---|
| Who decides | the consumer (binding), within the producer's key format | the producer (topic layout) |
| A new team / slice | one `queue_bind`, nothing else changes | filter in PHP, or a new topic + producer change |
| Cost of filtering | the broker matches, sends only matches | every group reads the whole topic |
| One message, many teams | copied into each matching queue | one copy; each group keeps its own cursor |
| Late subscriber | gets only what's published after the bind | can read history (retention permitting) |
| Nothing matches | `mandatory` → `312 NO_ROUTE`, or an alternate exchange | missing topic: an error; unread topic: no signal |

### Remember

- **`*` is exactly one word, `#` is zero or more.** `orders.*` got 0 of the 8 orders. One more word in the key silently breaks `orders.*.vn`; bind `orders.*.vn.#` if the key may grow.
- **One publish can land in many queues:** order 3 (`orders.paid.vn`) reached vn-team, finance and audit. 8 publishes, 16 deliveries.
- **A binding is broker state:** with the consumer stopped, its queue still collects messages.
- **An exchange stores nothing:** a queue bound now (`refunds`) gets no history. Unbind just cuts the route; the queue and its consumer stay.
- **A publisher confirm says ack even when no queue got the message.** Only `mandatory` (or an alternate exchange) tells you.
- **Kafka slices by topic, not partition:** partitions are for parallelism, chosen by `hash(key)` and handed out by the group. 3 events × 2 countries = 6 topics the producer must know in advance.
- **Regex subscription (KIP-848):** the broker matches the **whole** topic name; a new matching topic is picked up after ~6-13 s.

## Tiếng Việt

**Một câu:** ở RabbitMQ, **consumer** quyết định mình nhận gì: nó tự bind một pattern lên các word của routing key, và broker lọc giùm. Ở Kafka, **producer** quyết định qua cách chia topic: broker không lọc theo nội dung, nên consumer hoặc đọc hết rồi tự lọc trong PHP, hoặc phải có thêm topic.

| | RabbitMQ | Kafka |
|---|---|---|
| Ai quyết định | consumer (binding), trong khuôn key mà producer đặt | producer (bố cục topic) |
| Thêm team / slice mới | một `queue_bind`, không phải sửa gì khác | lọc trong PHP, hoặc tạo topic mới + sửa producer |
| Chi phí lọc | broker so khớp, chỉ gửi message khớp | mỗi group đọc toàn bộ topic |
| 1 message, nhiều team | copy vào từng queue khớp | chỉ 1 bản, mỗi group có con trỏ riêng |
| Consumer đến muộn | chỉ nhận message publish sau khi bind | đọc lại được lịch sử (trong thời hạn retention) |
| Không khớp gì | `mandatory` → `312 NO_ROUTE`, hoặc dùng alternate exchange | topic không tồn tại thì báo lỗi; topic không ai đọc thì không có tín hiệu nào |

### Ghi nhớ

- **`*` = đúng 1 word, `#` = 0 hoặc nhiều word.** `orders.*` nhận 0/8 order. Key dài thêm một word là `orders.*.vn` gãy âm thầm, nên dùng `orders.*.vn.#` nếu key có thể dài thêm.
- **Một lần publish có thể vào nhiều queue:** order 3 (`orders.paid.vn`) vào cả vn-team, finance và audit. 8 lần publish, 16 lần giao.
- **Binding là trạng thái trên broker:** consumer tắt thì queue vẫn tiếp tục giữ message.
- **Exchange không lưu gì:** queue bind sau (`refunds`) không có lịch sử. Unbind chỉ cắt đường đi; queue và consumer vẫn còn.
- **Publisher confirm vẫn báo `ack` dù không queue nào nhận message.** Chỉ `mandatory` (hoặc alternate exchange) mới cho bạn biết.
- **Kafka chia slice bằng topic, không bằng partition:** partition dùng để chạy song song, producer chọn bằng `hash(key)`, group tự chia cho các member. 3 event × 2 nước = 6 topic mà producer phải biết trước.
- **Regex subscription (KIP-848):** broker so regex với **toàn bộ** tên topic; topic mới khớp được nhận sau ~6-13 s.
