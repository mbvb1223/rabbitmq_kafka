# Demo 01: conclusions / kết luận

[English](#english) · [Tiếng Việt](#tiếng-việt)

## English

**One sentence:** a queue treats a message as a **work item** (done means deleted), a stream/log treats it as a **position** in a log (still there after reading, anyone can re-read it).

| | RabbitMQ queue | RabbitMQ stream | Kafka |
|---|---|---|---|
| After ack / commit | message deleted | nothing deleted | only the group's offset moves |
| Many consumers | split per message | everyone gets everything | same group: split per partition. Different groups: everyone gets everything |
| Max busy workers | any number | n/a | the partition count |
| Order | none across workers | publish order | per partition only |
| Replay | not possible | consumer passes `x-stream-offset` | broker stores an offset per group |

### Remember

- **Queue:** each message goes to 1 consumer, ack deletes it, a late consumer can't re-read it.
- **Kafka is always a log.** Its "queue" is just consumers sharing a group to split the work, so there is no per-message retry, DLQ or TTL like in RabbitMQ.
- **A group splits by partition, not by who is free.** Each partition belongs to one member, so a slow consumer blocks its partition and nobody else can help.
- **Who decides the split:**
  - Partition count: when the topic is created (`reset.sh` → `reset-topic qs.events 2`).
  - Which partition an event goes to: the producer (`hash(key) % partitions`; this demo pins it with `(i-1) % 2`).
  - Which consumer gets which partition: the broker (`group.protocol=consumer`, KIP-848).
- **`auto.offset.reset` (`first`) only applies when the group has no committed offset.** A group with an offset always resumes from it (D in Act 1). To re-read, use a new group, or `--reset-offsets` on the old one (stop all its consumers first).
- **More Kafka parallelism needs more partitions.** That's the price of per-partition ordering (demo 02).

## Tiếng Việt

**Một câu:** queue coi message là **việc cần làm** (làm xong thì xoá), stream/log coi message là **một vị trí** trong log (đọc xong vẫn còn, ai cũng đọc lại được).

| | RabbitMQ queue | RabbitMQ stream | Kafka |
|---|---|---|---|
| Ack / commit xong | message bị xoá | không xoá gì | chỉ dời offset của group |
| Nhiều consumer | chia nhau từng message | ai cũng nhận hết | cùng group: chia theo partition. Khác group: ai cũng nhận hết |
| Số worker có việc tối đa | không giới hạn | không áp dụng | bằng số partition |
| Thứ tự | không đảm bảo giữa các worker | đúng thứ tự publish | chỉ trong từng partition |
| Đọc lại | không được | consumer tự truyền `x-stream-offset` | broker lưu offset cho từng group |

### Ghi nhớ

- **Queue:** mỗi message chỉ tới 1 consumer, ack là xoá, consumer đến sau không đọc lại được.
- **Kafka luôn là log.** "Queue" trong Kafka chỉ là cách dùng chung một group để chia việc, nên không có retry, DLQ hay TTL cho từng message như RabbitMQ.
- **Cùng group thì chia theo partition, không theo ai rảnh.** Mỗi partition chỉ thuộc 1 member, nên consumer chậm sẽ chặn partition của nó và người khác không vào giúp được.
- **Ai quyết định chia:**
  - Số partition: lúc tạo topic (`reset.sh` → `reset-topic qs.events 2`).
  - Event vào partition nào: producer (`hash(key) % số partition`; demo này gán tay bằng `(i-1) % 2`).
  - Consumer nào nhận partition nào: broker (`group.protocol=consumer`, KIP-848).
- **`auto.offset.reset` (`first`) chỉ có tác dụng khi group chưa commit gì.** Group đã có offset thì luôn đọc tiếp từ đó (D ở Act 1). Muốn đọc lại thì dùng group mới, hoặc `--reset-offsets` trên group cũ (phải tắt hết consumer trước).
- **Muốn chạy song song nhiều hơn trong Kafka thì phải tăng số partition.** Đó là cái giá của việc giữ thứ tự theo partition (demo 02).
