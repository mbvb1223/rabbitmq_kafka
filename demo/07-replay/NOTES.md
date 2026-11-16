# Demo 07: conclusions / kết luận

[English](#english) · [Tiếng Việt](#tiếng-việt)

## English

**One sentence:** both replay, because a stream or a log keeps its messages. The difference is who remembers the position: Kafka stores each group's offset on the broker, while a RabbitMQ stream consumed over AMQP 0-9-1 (PHP) leaves it to your app. Retention limits how far back either can go.

| | RabbitMQ stream (AMQP 0-9-1, PHP) | Kafka |
|---|---|---|
| Who stores the position | your app: a file or DB row (`billing.offset` = last processed) | the broker, per group and partition (committed = next to read) |
| Resume after restart | pass saved + 1 as `x-stream-offset` | automatic, from the committed offset |
| Position lost | `next` misses what arrived while down; `first` redoes everything | `auto.offset.reset` decides (after 7 days as an empty group) |
| Rewind the live consumer | edit your stored offset | reset the group's offset (CLI, kafka-ui or code); the group must be EMPTY |
| One-off reprocess | consume with `x-stream-offset` = a time or offset, store nothing | `offsetsForTimes()` + `assign()`, no commit |
| Replay by time | starts at the chunk (a few older messages) | exact, per record timestamp |
| Replay limit | `x-max-age` / `x-max-length-bytes` | `retention.ms` / `retention.bytes` |

### Remember

- **The type of `x-stream-offset` decides its meaning:** strings `first` / `last` / `next` / `30s`, an int = an offset, a `DateTimeImmutable` = a timestamp. A unix time sent as an int is read as an offset: nothing arrives, ever, with no error.
- **At-least-once:** save the offset after the work. A kill between the work and the save replays one message, never skips one. In our run the Ctrl-C landed after the save (file = 4), so nothing replayed. The window is the 1.5 s of work.
- **Two conventions:** Kafka commits the next offset to read (5), our file stores the last processed (4). Mix them up and you skip or replay one message.
- **Rewinding changes the group's committed offset, not the topic.** The data stays, other groups are untouched. Four kinds: earliest / latest, timestamp (`--to-datetime`, `--by-duration PT10M`), offset (`--to-offset`, `--shift-by -2`). Run `--dry-run` before `--execute`.
- **An external reset needs an EMPTY group:** refused while the group is `STABLE`, whether from the CLI, kafka-ui or code. Stop (scale to 0) every consumer first.
- **On prod, nobody types CLI by hand.** From PHP (php-rdkafka has no Admin API): a command that, with the group empty, runs `offsetsForTimes()` and `commit()`s explicit offsets from a `KafkaConsumer` that never subscribes. Verified here: the commit set group `replay.scratch` to offset 3. Run it as a pipeline job: dry-run, review, execute, scale back up. kafka-ui has it too (Consumers → group → ⋮ → Reset offset: Earliest / Latest / Timestamp / Offset), but needs auth + RBAC before prod.
- **The consumer needs no rewind code; it needs idempotent processing** (upsert by event id), or a replay bills twice.
- **One-off reprocess (`reprocess.php`):** `assign()` instead of `subscribe()` and no commit, so it joins no group, doesn't need billing stopped, and leaves `replay.billing` untouched. It reads up to the end as of its start. Its own side effects (DB writes, API calls) must still be idempotent.
- **Retention is the replay window on both sides.** Past it, the data is gone.
- **"RabbitMQ doesn't store the offset" is a PHP limit, not a RabbitMQ one.** A stream can be read over AMQP 0-9-1 (port 5672, php-amqplib: the app stores the offset) or over the native stream protocol (port 5552: a named consumer + `storeOffset()` / `queryOffset()` on the broker, plus super streams and dedup). The stream protocol has official clients for Java, .NET and Go; PHP has only an experimental one (`crazy-goat/rabbit-stream`). The comparison depends on the client library too; collected in [99-final §5](../99-final/README.md#5-it-depends-on-the-client-not-only-the-broker).

## Tiếng Việt

**Một câu:** cả hai đều replay được, vì stream và log đều giữ lại message. Khác nhau ở chỗ ai nhớ vị trí đọc: Kafka lưu offset của từng group trên broker, còn RabbitMQ stream đọc qua AMQP 0-9-1 (PHP) thì để app tự lưu. Retention giới hạn việc tua lại được bao xa ở cả hai bên.

| | RabbitMQ stream (AMQP 0-9-1, PHP) | Kafka |
|---|---|---|
| Ai lưu vị trí | app của bạn: file hoặc row DB (`billing.offset` = offset cuối đã xử lý) | broker, theo từng group và partition (committed = offset kế tiếp cần đọc) |
| Resume sau restart | truyền saved + 1 vào `x-stream-offset` | tự động, từ offset đã commit |
| Mất vị trí | `next` mất những gì đến lúc consumer tắt; `first` làm lại toàn bộ | `auto.offset.reset` quyết định (khi group rỗng quá 7 ngày) |
| Tua consumer đang dùng | sửa offset mình lưu | reset offset của group (CLI, kafka-ui hoặc code); group phải EMPTY |
| Chạy lại một lần | consume với `x-stream-offset` = thời gian hoặc offset, không lưu gì | `offsetsForTimes()` + `assign()`, không commit |
| Replay theo thời gian | bắt đầu từ đầu chunk (dư vài message cũ) | chính xác, theo timestamp từng record |
| Giới hạn replay | `x-max-age` / `x-max-length-bytes` | `retention.ms` / `retention.bytes` |

### Ghi nhớ

- **Kiểu dữ liệu của `x-stream-offset` quyết định ý nghĩa:** chuỗi `first` / `last` / `next` / `30s`, số nguyên = offset, `DateTimeImmutable` = timestamp. Gửi unix time dạng số nguyên thì bị hiểu là offset: không nhận được gì, mãi mãi, không báo lỗi.
- **At-least-once:** lưu offset sau khi xử lý xong. Bị kill giữa lúc xử lý và lúc lưu thì chạy lại một message, không bao giờ bỏ sót. Lần chạy của mình Ctrl-C sau khi đã lưu (file = 4) nên không có gì chạy lại. Khoảng hở chính là 1.5 s xử lý.
- **Hai quy ước:** Kafka commit offset kế tiếp cần đọc (5), file của mình lưu offset cuối đã xử lý (4). Lẫn lộn là bỏ sót hoặc chạy lại một message.
- **Tua lại là đổi offset đã commit của group, không đụng topic.** Data giữ nguyên, group khác không bị ảnh hưởng. Bốn kiểu: earliest / latest, timestamp (`--to-datetime`, `--by-duration PT10M`), offset (`--to-offset`, `--shift-by -2`). Chạy `--dry-run` trước `--execute`.
- **Reset từ bên ngoài cần group EMPTY:** bị từ chối khi group đang `STABLE`, dù làm bằng CLI, kafka-ui hay code. Dừng (scale về 0) mọi consumer trước.
- **Trên prod không ai gõ CLI bằng tay.** Từ PHP (php-rdkafka không có Admin API): viết command, khi group rỗng thì chạy `offsetsForTimes()` rồi `commit()` offset cụ thể từ một `KafkaConsumer` không subscribe. Đã test ở đây: commit đặt group `replay.scratch` về offset 3. Chạy nó như một job trong pipeline: dry-run, review, execute, scale lên lại. kafka-ui cũng có (Consumers → group → ⋮ → Reset offset: Earliest / Latest / Timestamp / Offset), nhưng phải bật auth + RBAC trước khi dùng trên prod.
- **Consumer không cần code tua lại; nó cần xử lý idempotent** (upsert theo event id), không thì replay là tính tiền 2 lần.
- **Chạy lại một lần (`reprocess.php`):** `assign()` thay vì `subscribe()` và không commit, nên không join group, không cần dừng billing, `replay.billing` giữ nguyên. Nó đọc tới cuối topic tại thời điểm bắt đầu chạy. Side effect của chính nó (ghi DB, gọi API) vẫn phải idempotent.
- **Retention là giới hạn replay ở cả hai bên.** Quá mốc đó thì data đã bị xoá.
- **"RabbitMQ không lưu offset" là giới hạn của PHP, không phải của RabbitMQ.** Stream đọc được qua AMQP 0-9-1 (port 5672, php-amqplib: app tự lưu offset) hoặc qua stream protocol native (port 5552: consumer có tên + `storeOffset()` / `queryOffset()` trên broker, thêm super streams và chống trùng). Stream protocol có client chính thức cho Java, .NET, Go; PHP chỉ có một bản thử nghiệm (`crazy-goat/rabbit-stream`). Kết quả so sánh còn phụ thuộc vào thư viện client; tổng hợp ở [99-final §5](../99-final/README.md#5-it-depends-on-the-client-not-only-the-broker).
