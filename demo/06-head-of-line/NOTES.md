# Demo 06: conclusions / kết luận

[English](#english) · [Tiếng Việt](#tiếng-việt)

## English

**One sentence:** keeping order means no message may pass the one in front of it, so a slow or poison message blocks everything behind it. Letting others pass removes the blocking and the order with it. Ordering and head-of-line blocking are the same coin: you only choose where to pay.

| | RabbitMQ | Kafka |
|---|---|---|
| Progress is tracked as | an ack per message | one committed offset per partition |
| A slow job blocks | its worker + that worker's prefetch buffer; with single active consumer, the whole queue | every message behind it in the partition |
| Order | none across competing consumers; strict with single active consumer | per partition |
| Other workers | competing: take the rest of the queue; single active consumer: standbys | idle: they own other partitions |
| Poison message | requeued, then dropped or dead-lettered after `x-delivery-limit` (default 20) | read again after every restart, forever (LAG stuck at 4) |
| The fix | broker features: low prefetch, delivery limit + DLX | your code: catch, write to a DLQ topic, commit past it |

### Remember

- **Measure HOL as time since publish:** a 200 ms job that is `done 8 s after publish` waited behind the slow one. A late start (worker joining the group) inflates every job equally; that isn't HOL.
- **Prefetch 1:** unstarted jobs stay in the shared queue, so a free worker takes them and only job 3 is late. Completion order is 1, 2, 4 … 10, **3**.
- **Prefetch 10:** the broker deals messages round-robin to any consumer with a free slot. It counts unacked messages, it doesn't know a worker is busy. A held 1, 3, 5, 7, 9, so 5, 7, 9 waited 8 s while B sat idle. A delivered, unacked message belongs to that worker until ack, nack or disconnect. That's Kafka's partition 0.
- **Single active consumer:** one worker gets everything in order and the others wait (`true single_active` / `false waiting`). The active role doesn't rotate; publishing more changes nothing. Kill A mid-job: job 3 goes back to the head, B takes it as `(redelivered)`, then 4 … 10 in order. Hot standby, but no extra throughput, and the job runs twice, so handlers must be idempotent.
- **Kafka slow job:** the other worker finishes partition 1 and idles. It can't help, because one partition belongs to one member of the group. There's no prefetch knob: the partition is the buffer.
- **Kafka poison job:** the worker crashes without committing, restarts from the committed offset (job 3) and crashes again. Kafka keeps one integer, not a delivery count, so nothing stops the loop. Jobs 5, 7, 9 never ran.
- **DLQ mode:** copy job 3 to `hol.dlq`, commit only if that write succeeded, then move on. 5, 7, 9 ran 174 s after publish and LAG dropped to 0. Job 3 now runs after them, if at all: unblocking costs order again.
- **Order with more workers:** RabbitMQ `x-modulus-hash` + N single-active-consumer queues = per-key order. That rebuilds Kafka's partitions, blocking included.

## Tiếng Việt

**Một câu:** giữ thứ tự nghĩa là message phía sau không được vượt message phía trước, nên một message chậm hoặc lỗi sẽ chặn mọi thứ phía sau nó. Cho vượt thì hết bị chặn, nhưng mất luôn thứ tự. Ordering và head-of-line blocking là hai mặt của một đồng xu: bạn chỉ chọn trả giá ở đâu.

| | RabbitMQ | Kafka |
|---|---|---|
| Theo dõi tiến độ bằng | ack từng message | 1 offset đã commit cho mỗi partition |
| Job chậm chặn | worker giữ nó + bộ đệm prefetch của worker đó; với single active consumer thì chặn cả queue | mọi message phía sau nó trong partition |
| Thứ tự | không có giữa các competing consumer; strict với single active consumer | trong từng partition |
| Worker khác | competing: lấy phần còn lại của queue; single active consumer: đứng chờ dự phòng | ngồi không: chúng giữ partition khác |
| Poison message | được trả lại queue, quá `x-delivery-limit` (mặc định 20) thì bị bỏ hoặc dead-letter | restart lần nào cũng đọc lại, mãi mãi (LAG kẹt ở 4) |
| Cách sửa | tính năng của broker: prefetch thấp, delivery limit + DLX | code của bạn: bắt lỗi, ghi sang topic DLQ, commit vượt qua |

### Ghi nhớ

- **Đo HOL bằng thời gian từ lúc publish:** job 200 ms mà `done 8 s after publish` là đã phải chờ sau job chậm. Worker bật muộn (đang join group) làm mọi job cùng bị cộng thêm thời gian, đó không phải HOL.
- **Prefetch 1:** job chưa bắt đầu vẫn nằm trong queue chung, worker rảnh lấy tiếp, nên chỉ job 3 bị muộn. Thứ tự hoàn thành là 1, 2, 4 … 10, **3**.
- **Prefetch 10:** broker giao lần lượt, xoay vòng cho consumer nào còn slot trống. Nó chỉ đếm số message chưa ack, không biết worker đang bận. A giữ 1, 3, 5, 7, 9, nên 5, 7, 9 chờ 8 s trong khi B ngồi không. Message đã giao mà chưa ack thì thuộc về worker đó cho tới khi ack, nack hoặc mất kết nối. Giống hệt partition 0 của Kafka.
- **Single active consumer:** một worker nhận hết theo thứ tự, các worker khác đứng chờ (`true single_active` / `false waiting`). Vai active không xoay vòng, publish thêm cũng không đổi. Tắt A giữa job 3: job 3 về lại đầu queue, B nhận nó với `(redelivered)`, rồi 4 … 10 theo thứ tự. Có dự phòng nóng nhưng không tăng throughput, và job bị chạy 2 lần nên handler phải idempotent.
- **Kafka, job chậm:** worker kia làm xong partition 1 rồi ngồi không. Nó không giúp được vì mỗi partition chỉ thuộc 1 member trong group. Không có nút prefetch để chỉnh: partition chính là bộ đệm.
- **Kafka, poison job:** worker crash mà không commit, restart từ offset đã commit (job 3) rồi lại crash. Kafka chỉ lưu một con số offset, không đếm số lần giao, nên không có gì dừng vòng lặp. Job 5, 7, 9 không bao giờ chạy.
- **Mode DLQ:** copy job 3 sang `hol.dlq`, chỉ commit khi ghi thành công, rồi chạy tiếp. 5, 7, 9 xong sau 174 s tính từ lúc publish, LAG về 0. Job 3 giờ chạy sau chúng (nếu có ai xử lý DLQ): hết kẹt thì lại mất thứ tự.
- **Muốn giữ thứ tự mà nhiều worker:** RabbitMQ dùng `x-modulus-hash` + N queue single-active-consumer = thứ tự theo key. Như vậy là dựng lại partition của Kafka, kèm luôn blocking.
