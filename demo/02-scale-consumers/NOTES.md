# Demo 02: conclusions / kết luận

[English](#english) · [Tiếng Việt](#tiếng-việt)

## English

**One sentence:** RabbitMQ scales by adding workers; Kafka scales by adding workers **only up to the partition count**, and partitions only go up.

| | 1 worker | 4 workers | 20 workers |
|---|---|---|---|
| RabbitMQ (prefetch 10) | 200 jobs, ~10.4 s | 50 each, ~2.6 s | 10 each, ~0.5 s |
| RabbitMQ, backlog + prefetch 0 | 200, ~10.4 s | **200/0/0/0**, ~10.4 s | **200 + 19 idle**, ~10.4 s |
| Kafka, 4 partitions | 200 (owns p0-p3), ~10.9 s | 50 each, ~2.7 s | **4 busy, 16 idle**, ~2.6 s |
| Kafka, 20 partitions | | | 10 each, ~0.5 s |

### Remember

- **Prefetch (`basic_qos`)** is the most unacked messages the broker pushes to one consumer. Not calling it = unlimited: the first worker grabs the whole backlog and the others sit idle (they stay unacked, not deleted; if that worker dies they go back to the queue). **Always set it**: 1 for slow jobs, tens to hundreds for fast ones.
- The prefetch trap makes RabbitMQ look like Kafka's idle workers. "RabbitMQ doesn't scale" → check `basic_qos` first.
- **Kafka hands out whole partitions**, one member per partition per group, so max busy workers = partition count.
- **Kafka settle time:** new members get partitions on a heartbeat (every 5 s), so a group takes ~7-18 s to settle. RabbitMQ workers start immediately.
- **Why not always add partitions:**
  - Grow-only: 20 → 2 fails; shrinking means a new topic.
  - Keys move: `hash(key) % count` changes, so old events for key K wait in the old partition while new ones go to a new partition, and two workers can process K at once.
  - New partitions + `auto.offset.reset=latest` can skip the first records.
  - So size it up front. Unkeyed, independent jobs can simply over-partition.
- **The key is the unit of ordering.** Use `order_id` as the key and every step of one order (created → paid → shipped) lands in one partition and runs in order, while different orders run in parallel.
- **Partitions run in parallel, so order across keys is lost, but order within one key is kept.** That guarantee is what the partition ceiling pays for.

## Tiếng Việt

**Một câu:** RabbitMQ cứ thêm worker là nhanh thêm; Kafka thêm worker chỉ nhanh **tới khi bằng số partition**, mà số partition chỉ tăng được, không giảm được.

| | 1 worker | 4 worker | 20 worker |
|---|---|---|---|
| RabbitMQ (prefetch 10) | 200 job, ~10.4 s | 50 mỗi worker, ~2.6 s | 10 mỗi worker, ~0.5 s |
| RabbitMQ, tồn sẵn + prefetch 0 | 200, ~10.4 s | **200/0/0/0**, ~10.4 s | **200 + 19 ngồi không**, ~10.4 s |
| Kafka, 4 partition | 200 (giữ p0-p3), ~10.9 s | 50 mỗi worker, ~2.7 s | **4 có việc, 16 ngồi không**, ~2.6 s |
| Kafka, 20 partition | | | 10 mỗi worker, ~0.5 s |

### Ghi nhớ

- **Prefetch (`basic_qos`)** là số message chưa ack tối đa broker gửi trước cho một consumer. Không gọi `basic_qos` = không giới hạn: worker đầu tiên ôm hết message tồn, các worker khác ngồi không (message ở trạng thái unacked, chưa bị xoá; worker đó chết thì message quay về queue). **Lúc nào cũng set**: 1 cho job chậm, vài chục tới vài trăm cho job nhanh.
- Bẫy prefetch làm RabbitMQ trông giống Kafka có worker ngồi không. Ai bảo "RabbitMQ không scale" thì kiểm tra `basic_qos` trước.
- **Kafka giao nguyên partition**: trong một group, mỗi partition chỉ có 1 member, nên số worker có việc tối đa = số partition.
- **Kafka phải chờ group ổn định:** member mới nhận partition qua heartbeat (5 s một lần), nên mất ~7-18 s. RabbitMQ worker nhận việc ngay.
- **Vì sao không lúc nào cũng tăng partition:**
  - Chỉ tăng được: 20 → 2 sẽ báo lỗi; muốn giảm phải tạo topic mới.
  - Key bị chuyển: `hash(key) % số partition` thay đổi, event cũ của key K còn chờ ở partition cũ trong khi event mới vào partition mới, nên 2 worker có thể xử lý K cùng lúc.
  - Partition mới + `auto.offset.reset=latest` có thể bỏ sót các message đầu tiên.
  - Vì vậy phải chọn số partition từ đầu. Job không có key, độc lập với nhau thì cứ tạo dư partition.
- **Key là đơn vị giữ thứ tự.** Dùng `order_id` làm key thì mọi bước của một order (created → paid → shipped) nằm chung partition và chạy đúng thứ tự, còn các order khác nhau chạy song song.
- **Các partition chạy song song nên mất thứ tự giữa các key, nhưng trong cùng một key thì vẫn giữ thứ tự.** Giới hạn worker theo partition là cái giá để có đảm bảo đó.
