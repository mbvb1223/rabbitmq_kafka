# Demo 03: conclusions / kết luận

[English](#english) · [Tiếng Việt](#tiếng-việt)

## English

**One sentence:** RabbitMQ gives you retry, backoff and a DLQ as **5 queue arguments**; Kafka has none of it, so you **build it yourself**: a retry topic, a DLQ topic and a second consumer.

| | RabbitMQ | Kafka |
|---|---|---|
| Retry + backoff | the broker: 3, 6, 9 s | your code copies the job to `retry.jobs.retry`; one **fixed** delay per topic |
| Who counts attempts | the broker (`x-delivery-count`) | your own header (`x-attempt`) |
| DLQ | `x-dead-letter-exchange`, reason in `x-death` | your code produces to `retry.jobs.dlq` |
| Does a failed job block the rest? | no, the broker parks it | no, but only because your code copied it out and committed |
| You run | 1 worker | 2 consumers (worker + retrier) |
| Crash midway | retry: nothing to lose. Act 3: a duplicate in the DLQ | a duplicate (at-least-once); the consumer must dedupe |

### Remember

- **`reject`, not `nack`.** Since 4.3 a requeued `nack` isn't a failed delivery: `delivery-count` stays 0, so no backoff, no limit, and with prefetch 1 the jobs behind it starve.
- **`x-delivery-limit=3` means 4 deliveries** (1 attempt + 3 retries).
- **Backoff is linear:** `min(retry-min × delivery-count, retry-max)`, quorum queues only, set per queue.
- **Headers exchange: use `all-with-x`.** Plain `all` ignores `x-*` headers, so the binding has no criteria and matches everything. Add an `alternate-exchange` so an unknown reason still lands in a DLQ.
- **Kafka can't set one job aside:** a group keeps one offset per partition. Either wait on the job (blocks the partition) or copy it out, then commit.
- **One retry topic = one delay.** Real backoff needs one topic per step (`retry-3s`, `retry-6s`, ...).
- **Copy first, then ack/commit.** The DLQ/retry copy must be confirmed by the broker before you ack or commit the original: a crash then gives a duplicate, never a lost job.

## Tiếng Việt

**Một câu:** RabbitMQ có sẵn retry, backoff và DLQ, chỉ cần khai báo **5 argument cho queue**. Kafka thì không có, bạn phải **tự dựng**: retry topic, DLQ topic và thêm một consumer.

| | RabbitMQ | Kafka |
|---|---|---|
| Retry + backoff | broker tự làm: 3, 6, 9 s | tự viết code: copy job sang `retry.jobs.retry`, delay **cố định** mỗi topic |
| Đếm số lần thử | broker (`x-delivery-count`) | header tự đặt (`x-attempt`) |
| DLQ | `x-dead-letter-exchange`, lý do nằm trong `x-death` | code tự produce sang `retry.jobs.dlq` |
| Job lỗi có chặn job khác? | không, broker giữ nó lại | không, nhưng chỉ vì code đã copy nó ra rồi commit |
| Phải chạy | 1 worker | 2 consumer (worker + retrier) |
| Crash giữa chừng | retry: không có rủi ro. Act 3: DLQ bị trùng | bị trùng (at-least-once), consumer phải tự dedupe |

### Ghi nhớ

- **Dùng `reject`, không dùng `nack`.** Từ 4.3, `nack` có requeue không được tính là failed: `delivery-count` đứng ở 0, nên không có backoff, không có limit, và với prefetch 1 thì các job phía sau không được chạy.
- **`x-delivery-limit=3` nghĩa là 4 lần giao** (1 lần chạy + 3 lần retry).
- **Backoff tăng tuyến tính:** `min(retry-min × delivery-count, retry-max)`, chỉ có ở quorum queue, đặt cho cả queue.
- **Headers exchange: dùng `all-with-x`.** `all` bỏ qua các header `x-*`, nên binding không còn điều kiện nào và khớp mọi message. Thêm `alternate-exchange` để lý do lạ vẫn rơi vào một DLQ.
- **Kafka không để một job sang một bên được:** group chỉ giữ 1 offset cho mỗi partition. Hoặc là chờ job đó (chặn cả partition), hoặc copy nó ra ngoài rồi commit.
- **Một retry topic = một mức delay.** Muốn backoff tăng dần thì cần một topic cho mỗi mức (`retry-3s`, `retry-6s`, ...).
- **Copy trước, ack/commit sau.** Bản copy vào DLQ hoặc retry topic phải được broker xác nhận rồi mới ack/commit job gốc: crash thì chỉ bị trùng job, không bao giờ mất job.
