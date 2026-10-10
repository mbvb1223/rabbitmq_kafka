# Demo review (2026-10-10)

Static review of demos 01-10, 99-final and the shared `lib/`, `bin/`, `compose.yaml`, `README.md`. 5 reviewers, 2 demos each. Nothing was run and no file was changed.

Criteria:
- **Code:** simple, clean, only the lesson. Production hardening counts as noise.
- **README:** step by step, short. Background goes to a `## More details` section at the end.

> Demos 01, 05, 08 and 09 had uncommitted edits while the review ran (13:40-14:05). Findings marked ~~struck~~ were already fixed in the working tree. Re-check the others in those demos.

---

## 1. Summary

**No code blockers. 1 README blocker (demo 10). 5 more fixes needed before you learn the demo.** Every README is about 2× longer than it needs to be.

| Demo | Code | README now → target | Fix first |
|---|---|---|---|
| 01 queue-vs-stream | OK, publisher noise | 154 → ~75 lines | — |
| 02 scale-consumers | **Major**: `fleet.php` harness heavier than the lesson | 144 → ~65 | — |
| 03 retry-dlq | OK, Kafka helpers over-hardened | 188 → ~85 | **README:77** reset hidden in a comment |
| 04 routing | OK (nits only) | 153 → ~75 | — |
| 05 ttl-priority | OK, 2 small cleanups | 162 → ~70 | — |
| 06 head-of-line | OK | 146 → ~75 | **README:94-96** claim depends on fetch order |
| 07 replay | OK | 164 → ~85 | — |
| 08 share-groups | OK, a few dead bits | 159 → ~80 | **README:97-102** 40 s window not in README |
| 09 log-compaction | OK, 2 duplicated loops | 161 → ~75 | — |
| 10 durability | **Major**: reset retry loop never retries | 206 → ~100 | **Blocker** README:53-72 missing restart + verify |
| 99 final | — | 81 lines, dense | **:19** stale "~500 ms" number |
| Shared | OK, nits | 64 → ~45 | add host prerequisites line |

### Fix first (these break the lesson if you follow the README as written)

| # | Severity | Where | Issue |
|---|---|---|---|
| 1 | Blocker | `10-durability/README.md:53-55, 69-72` | The Act 1 stall run and Act 2 leave out `docker compose --profile kafka start` and `kafka/run verify.php <topic>`. The LOST lines come from verify. Act 2 then starts with 2 brokers, `kill-leader` leaves 1, which is below `min.insync=2`, so you get timeouts instead of LOST 0. |
| 2 | Major | `10-durability/kafka-cli:6-8` + `reset.sh:12-13` | The pipeline's exit code comes from `grep -v`, which is 0 whenever any line is printed, error lines included. A failed `--create` right after `up -d` ends the `until` loop, the topic is silently missing, and `produce.php:31` fails with a fatal error. Fix: `until ./kafka-cli kafka-topics --create ... \| grep -q 'Created topic'; do sleep 1; done` |
| 3 | Major | `03-retry-dlq/README.md:77` | "then Ctrl-C the worker and ../reset.sh" sits inside a code comment. If you skip it, jobs 3-5 stay in `retry.jobs` and Act 3's output changes. Make it its own step. |
| 4 | Major | `08-share-groups/README.md:97-102` | Act 3 only works if you publish within ~40 s of "frozen". `stuck-worker.sh:16` now prints this; the README step still doesn't. |
| 5 | Minor | `06-head-of-line/README.md:94,96` | "Run 1 finishes partition 1 / LAG 0" depends on which partition librdkafka fetches first. Write: "partition 1 may stall too: the crash takes the whole worker down". |
| 6 | Major | `99-final/README.md:19` | It still says "rebuild state in ~500 ms". 09 now says that number is connect overhead. |

---

## 2. Patterns across all demos (fix once, apply everywhere)

| # | Pattern | Demos | Fix |
|---|---|---|---|
| P1 | An intro table before Setup repeats the Compare table at the end, sometimes word for word | 01, 02, 03, 04, 06, 07, 08, 09, 10 | Delete the intro table. Keep Compare at the end. |
| P2 | Three intros (prose + "You'll learn" + table) before the first command | all | A **2-line Goal** only |
| P3 | Gotchas / Talking points repeat the Acts, code comments or the root README | all | Delete the duplicates. Move what's new into `## More details`. |
| P4 | Steps hidden in bullets or prose instead of code blocks | 03, 04, 05, 06, 08, 10 | Every action is a command in a code block, labelled `# terminal N` |
| P5 | Background mixed into "what you see" bullets (KIP numbers, timings from specific runs, internals) | all | One "what you see" bullet per point. The rest goes to More details. |
| P6 | Publisher noise unrelated to the lesson: confirms + nack handler, `delivery_mode`, `enable.idempotence`, DR callback + `$failed` | 01, 03, 04, 05, 06, 09 | Drop them. Keep them only in demo 10, where durability *is* the lesson. |
| P7 | Hand-written EOF/timeout loop that duplicates `kafka_message()` | 03 `dlq.php`, 05 `peek.php`, 09 `read.php`/`rebuild.php`, 10 `verify.php` | `while ($msg = kafka_message($consumer->consume(5000))) { ... }` |
| P8 | `match` + `InvalidArgumentException` for an argument with only 2 values | 01 `bootstrap.php`, 06 `bootstrap.php` | `$name = "qs.$mode";` |
| P9 | CLI arguments/defaults the README never uses | 04, 05, 06, 07, 08 | Hardcode the value |
| P10 | `reset.sh` doesn't delete the demo's consumer groups (root README:61 says every reset does) | 03, 06 (~~05~~ fixed) | Copy 04's group-delete loop |
| P11 | Comparing against the display string `'none (idle)'` from `partitions()` | 02 `bench.php:31`, 05 `consume.php:31` | Use `!$consumer->getAssignment()` |
| P12 | The "UIs: ..." line repeats the root README | 01, 02, 03, 04, 08 | Delete it |

### Recommended README template

```markdown
# Demo NN: <name> (~N min)

<Goal: 2 lines. The one claim this demo proves.>

## Setup
./reset.sh

## RabbitMQ (cd rabbitmq)
### Step 1: <what>
​```bash
php ...   # terminal 1
php ...   # terminal 2
​```
- You see: <one line>

### Step 2: ...

## Kafka (cd kafka)
### Step N: ...

## Compare
| | RabbitMQ | Kafka |   <- ≤6 rows, ≤8 words per cell

## Checklist
- [ ] ...

## More details
<client versions, internals, KIPs, timings, gotchas, talking points>
```

---

## 3. Per-demo findings

## Demo 01: queue-vs-stream
**Verdict:** Code OK, publishers carry production noise. README 154 lines (~8 min) → ~75 (~3 min).

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Minor | `rabbitmq/publish.php:12-14,24` | Publisher confirms + nack handler aren't this lesson (that's demo 10). Demo 02 has no nack handler, so the two are inconsistent. | Drop `confirm_select`, `set_nack_handler`, `wait_for_pending_acks`. Also drop `delivery_mode` (`:19`): quorum queues and streams always persist. |
| 2 | Minor | `kafka/publish.php:8-15,28-30` | DR callback, `$failed` counter, `enable.idempotence`: 8 lines the learner has to skip | `new RdKafka\Producer(kafka_conf())`. Keep only the `flush()` check. |
| 3 | Minor | `reset.sh:5,11` | `pkill` and the share-group delete exist only for the off-lesson Bonus | Delete them together with the Bonus |
| 4 | Nit | `kafka/publish.php:22` | Key `order-$i` is ignored (explicit partition) and suggests keys matter | Drop the key |
| 5 | Nit | `rabbitmq/bootstrap.php:8-9,24-31` | `target()`, 2 constants and an exception for a 2-value argument | `$name = "qs.$mode";` |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:3-19` | 3 intros before the first command. The table repeats Compare `:124-131`; `:17` has client versions. | 2-line goal + 3 learn bullets. Versions go to More details. |
| 2 | Major | `:75-77` | 2 paragraphs on assignment mechanics. Act 2 (`:96-99`) never says to wait. | "Wait until every consumer prints `partitions: …` (~7 s) before publishing." |
| 3 | Minor | `:104-106` | The rewind uses D from Act 1 in the middle of Act 2. D also eats Act 2's publish, which isn't mentioned. | Move the rewind to the end of Act 1 |
| 4 | Minor | `:59-64` | 4 `stream C` runs in a row; each blocks, and "Ctrl-C between" is never said | Add "Ctrl-C C after each". Keep 2 rows (`first`, `3`). |
| 5 | Minor | `:66-69` | Time offset + chunk caveat is a second lesson | Keep "Ctrl-C A, publish 3, restart A → none". Move `30s`/chunks to More details. |
| 6 | Minor | `:108-118` | Share-group Bonus is off-lesson and repeats demo 08 | One Compare line: "Kafka's real queue → demo 08" |
| 7 | Minor | `:133-146` | Talking points + Gotchas repeat Compare and code comments | Move to More details, dedupe |
| 8 | Nit | `:44,56,89-90` | Background inside steps (disk reclaim, credit, key hashing) | Keep only what you see on screen |

### Suggested outline
- **Goal:** a queue message is a work item (ack deletes it); a log message is a position (commit only moves a cursor).
- **Setup:** Ctrl-C all consumers, then `./reset.sh`.
- **RabbitMQ:** S1 queue (A/B split, UI 0, late C gets nothing) → S2 stream (A and B both get 0-5, UI 6) → S3 replay `first` / `3` → S4 no stored position.
- **Kafka** (wait for `partitions:` before each publish): S5 same group (C idle) → S6 resume → S7 rewind `--by-duration` → S8 different groups.
- **Compare** (+ share groups → demo 08), **Checklist**, **More details**.

## Demo 02: scale-consumers
**Verdict:** Code works, but the fork harness is heavier than the lesson. README 144 lines (~8 min) → ~65 (~3 min).

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `fleet.php:1-140` | 140-line harness (socket pairs, line buffers, `stream_select`, signal relay, bar chart) is longer than both `bench.php` files together | Treat it as a black box in the README ("you don't need to read this"). Trim: drop the `#` bars (`:110,115`); `owned()` → `count(array_merge(...$this->assigned))`. Optional: one shared DGRAM socket pair. |
| 2 | Minor | `kafka/bench.php:31` | Calls `getAssignment()` twice to dodge the `'none (idle)'` sentinel | `implode(',', array_map(fn ($tp) => $tp->getPartition(), $consumer->getAssignment()))` |
| 3 | Nit | `rabbitmq/bench.php:14-21` | `match(true)` with assignments as side effects, just to parse arguments | `$workers = (int) $argv[1]; $backlog = in_array('--backlog', $argv);` + one `if` |
| 4 | Nit | `rabbitmq/bench.php:50` | Each worker calls `declare_queue()` again; the parent already declared it at `:72` | Use `rabbit()` in the worker |
| 5 | Nit | `kafka/bench.php:50-51,64` | `newTopic` twice, 4-method metadata chain, `enable.idempotence` | Create `$topic` once, drop idempotence |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:28-30, :72-74` | Mechanics prose before the first command; the KIP-848 heartbeat is explained twice | One line on what `bench.php N` does + "host PHP needs pcntl + posix" |
| 2 | Minor | `:12-16` | Intro table overlaps Compare | Delete (P1) |
| 3 | Minor | `:57-62` | One code block mixes 2 sequential runs with a 2-terminal run | Split into "prefetch 0 vs 10" and an optional "watch 20 workers" step |
| 4 | Minor | `:91` | The `--describe` window is ~8 s and `bin/kafka` takes 2-3 s to start | Mark it optional |
| 5 | Minor | `:100-107, :124-125` | Nested theory bullets, share groups, single active consumer / consistent hash | One-liners: "grow-only", "keys remap". The rest goes to More details. |
| 6 | Minor | `:128-136` | Gotchas are build notes that repeat code comments | Move to More details |

### Suggested outline
- **Goal:** on RabbitMQ, more PHP processes means more throughput; on Kafka that stops at the partition count.
- **Setup:** `./reset.sh`; one line on what bench does.
- **RabbitMQ:** S1 `bench.php 1 / 4 / 20` → S2 prefetch trap (`--prefetch=0` 200/0/0/0 vs ~50 each) → S3 optional watch.
- **Kafka:** S4 `bench 1 / 4 / 20` (16 idle) → S5 optional `--describe` → S6 `--alter --partitions 20` and rerun; `../reset.sh` restores 4.
- **Compare**, **Checklist**, **More details** (prefetch semantics, KIP-848 settle, how `fleet.php` works, build gotchas).

## Demo 03: retry-dlq
**Verdict:** Code works and outputs match; the Kafka helpers carry production hardening. README 188 lines (~10 min) → ~85 (~4 min).

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Minor | `kafka/common.php:23-49` | `send()` has a static topic cache plus `$GLOBALS['dr_error']` via a DR callback. The lesson is just "copy, flush, then commit". | `$p->newTopic($topic)->producev(RD_KAFKA_PARTITION_UA, 0, $msg->payload, $msg->key, $headers); $p->flush(10_000);` with the comment "copy on broker before commit" |
| 2 | Minor | `kafka/dlq.php:14-21` | Rewrites `kafka_message()` by hand | P7 |
| 3 | Minor | `kafka/retrier.php:17-25` | Sleeps in 200 ms chunks just so Ctrl-C is clean; a 3-line comment repeats README:145 | `usleep(max(0, (int) $h['x-due-at'] - now_ms()) * 1000);` |
| 4 | Minor | `reset.sh` | Doesn't delete the `retry.*` groups | P10 |
| 5 | Nit | `kafka/worker.php:20` vs `retrier.php:34,37` | `$h + [...]` vs `[...] + $h`: the learner has to know that PHP `+` keeps the left-hand keys | Use `[...] + $h` in both |
| 6 | Nit | `rabbitmq/worker.php:12-14` | Confirm + a throwing nack handler run in every mode; only `route` publishes | Drop the nack handler |
| 7 | Nit | `rabbitmq/publish.php:14`, `kafka/publish.php:10` | `+ [1 => 'timeout']` fallback only so the README can write `publish.php 5 3` | Drop it; README uses `php publish.php` |
| 8 | Nit | `reset.sh:7-8` | Exchange delete is a leftover from building the demo | Drop it |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:77` | Reset hidden in a command comment (Fix first #3) | Its own step |
| 2 | Major | whole file | nack point said 6×, "limit 3 = 4 deliveries" 3×, `all-with-x` 4×, backoff formula 4× | Say each once, in its Act |
| 3 | Major | whole file | Talking points + Gotchas come before the Checklist | Use the template |
| 4 | Minor | `:15-19` | Intro table = Compare | P1 |
| 5 | Minor | `:120` | "Wait until both print `waiting on`" prints before the group join | "Start both, wait ~5 s, then publish." |
| 6 | Minor | `:160-164` | The crash row is a paragraph; JVM frameworks / KIP-1191 are background | Short cells; move the rest |

### Suggested outline
- **Goal:** one failing job is retried with backoff and then dead-lettered, without blocking others. RabbitMQ = 5 queue arguments; Kafka = retry/DLQ topics + a second consumer you write.
- **Setup:** `./reset.sh`
- **RabbitMQ:** 5-row argument table → S1 `worker.php reject` + `publish` + `dead.php` → S2 `worker.php nack` (count stays 0, 4-5 starve) → **S3 `../reset.sh`** → S4 `worker.php route` with `3:timeout,4:validation`.
- **Kafka:** S5 `worker.php` + `retrier.php`, wait 5 s, `publish.php`, `dlq.php`.
- **Compare** (6 short rows), **Checklist**, **More details** (`x-acquired-count`, 4.3 nack, `pausePartitions`, at-least-once, JVM frameworks, KIP-1191).

## Demo 04: routing
**Verdict:** Code OK, nits only; every count and output matches. README 153 lines (~8 min) → ~75 (~3 min).

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Nit | `rabbitmq/consume.php:9-10,17-19` + `unbind.php:6`, `filter.php:6`, `regex.php:6` | Multi-pattern support and defaults the README never uses | `[, $team, $pattern] = $argv;` + one `queue_bind` |
| 2 | Nit | `rabbitmq/consume.php:20` | `basic_qos(0, 10)` isn't part of routing | Drop |
| 3 | Nit | `rabbitmq/publish.php:14,22` | Nack handler + persistent mode | Drop both; keep the ack handler (Act 3 needs it) |
| 4 | Nit | `kafka/publish.php:12` | `enable.idempotence` | Keep only the 2 timeout settings |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:46` | A whole step buried in a bullet (Ctrl-C audit, publish, restart, prints the 8 it missed) | Numbered step with commands |
| 2 | Major | `:84` | The late `refunds` filter is a step written as a bullet | Put it in the code block |
| 3 | Major | whole file | Talking points + 8 Gotchas before the Checklist | Use the template |
| 4 | Minor | `:78, :91` | Kafka acts reuse terminals still running earlier consumers | "Ctrl-C the consumers" before each Kafka act |
| 5 | Minor | `:3-5` | 2 prose paragraphs, the second is a glossary | 2-line goal + 4 glossary bullets |
| 6 | Minor | `:13,112,141,152` | Regex pickup time said 4×, as "~15 s" vs "6-13 s" | Once, "6-13 s" |

### Suggested outline
- **Goal:** RabbitMQ: the consumer picks its slice with a binding. Kafka: the producer's topic layout decides, or the consumer filters in PHP.
- **Glossary** (4 bullets), **Setup**.
- **RabbitMQ:** S1 3 consumers + publish (vn 1,3,6,7,8; finance 3,4,7; audit 8) → S2 audit offline then restart (gets the 8) → S3 refunds + `unbind.php` → S4 `NO_ROUTE` on a typo.
- **Kafka** (Ctrl-C first): S5 2 filters (`kept 5` / `kept 3`) → S6 late refunds reads history → S7 regex + split topics + typo error.
- **Compare**, **Checklist**, **More details**.

## Demo 05: ttl-priority
**Verdict:** Code clean apart from 2 small simplifications. README ~162 lines (~7 min) → ~70 (~3 min). *(Edited during the review; re-check.)*

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Minor | `kafka/consume.php:31` | Wait loop compares against the `'none (idle)'` display string | `while ($running() && !$high->getAssignment())` |
| 2 | Minor | `kafka/peek.php:17-27` | Checks null/EOF/timeout by hand, then calls `kafka_message()` anyway | P7 |
| 3 | ~~Minor~~ | ~~`reset.sh`~~ | ~~doesn't delete `ttl.workers`~~ | fixed in working tree |
| 4 | Nit | `kafka/consume.php:12,19-24` | Manual commit after each message isn't the lesson | Drop `enable.auto.commit` + `commit()`; `process($msg)` |
| 5 | Nit | `rabbitmq/publish-ttl.php:14,18` | Unused `'timestamp'`; nested ternary with a hardcoded `10000` | Delete; `$body = 'job ' . ($i + 1) . " (ttl $ttl), published " . date('H:i:s');` |
| 6 | Nit | `kafka/peek.php:6` | Unused `[topic]` argument | Hardcode `ttl.events` |
| 7 | Nit | publishers, `watch-expired.php:9` | Confirms, `delivery_mode`, idempotence, `basic_qos` | P6 |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:5-18, :110-133` | Content repeated 3-4× (learn / intro table / Compare / Gotchas) | P1-P3 |
| 2 | Minor | `:64-65` | The 32-levels and prefetch-1-vs-10 experiments have no commands to copy; the Checklist depends on the second | `for p in 0 4 5 20 30 31 50 255; do php publish-prio.php 1 $p; done; php consume-prio.php` and `consume-prio.php 10` vs 1 |
| 3 | Minor | `:43, :86-89` | The `publish-ttl.php 20000` step is hidden in a bullet; no terminal labels | Put commands in blocks, label t1/t2 |
| 4 | Minor | `:78, :92-93` | "After 30 s all 5 are still there" isn't guaranteed (~7% chance); run-specific numbers | "usually still there"; "~5 min old" |
| 5 | Nit | `:37-49` | Doesn't say to keep `watch-expired.php` running for Act 2 | "(leave t1 running)" |

### Suggested outline
- **Goal:** RabbitMQ expiry and priority are per message. Kafka "TTL" is per-segment retention on a 5-min timer; priority is something you build from 2 topics.
- **RabbitMQ:** S1 TTL per message (shorter wins) → S2 expiry only at the head → S3 priority (p31 first) → S4 prefetch 10 cancels priority.
- **Kafka:** S5 retention isn't TTL (gone within ~5 min) → S6 one fresh record keeps old ones alive → S7 2-topic priority.
- **Compare** (4 rows), **Checklist**, **More details** (>31 levels, queue depth counts expired, `segment.ms`, starvation, `assign()` vs `subscribe()`).

## Demo 06: head-of-line
**Verdict:** Code fine apart from one helper to drop. README 146 lines (~9 min) → ~75 (~3.5 min). One Kafka claim depends on fetch order.

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Minor | `rabbitmq/bootstrap.php:22-29` | `queue()` uses `match` + exception for 2 modes | P8: `$name = "hol.$mode";` |
| 2 | Minor | `reset.sh:6-8` | Doesn't delete `hol.workers` | P10 |
| 3 | Nit | `kafka/consume.php:37-41` | The "interrupted mid-job" guard is never reached by any README step | Drop (optional) |
| 4 | Nit | `kafka/consume.php:7`, `rabbitmq/consume.php:8` | `'worker-' . getmypid()` default is never used | Default `'A'` |
| 5 | Nit | publishers + `kafka/consume.php:17` | Confirms, `delivery_mode`, idempotence | P6 |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:113-119` | Compare is too dense (the Poison cell is a paragraph) | 4 short rows |
| 2 | Major | `:3-16, :111-137` | "Same coin" restated 5×; Gotchas repeat the Acts | P1-P3 |
| 3 | Minor | `:94, :96` | Partition 1 outcome depends on fetch order (Fix first #5) | "partition 1 may stall too" |
| 4 | Minor | `:22` | "Reset before each act except Kafka Act 3", but only Kafka Act 2 shows it | Reset once in Setup + once in Kafka Act 2 |
| 5 | Minor | `:45, :64` | Act 2 commands written as prose; the failover experiment is hidden in a bullet | Code blocks, own step |
| 6 | Nit | `:142` | "while B is idle", but the even worker can be A or B | "the other worker is idle" |

### Suggested outline
- **Goal:** Kafka commits one offset per partition, so a slow or poison job blocks its neighbours. RabbitMQ acks per message and routes around it, until you ask for order.
- **RabbitMQ:** S1 prefetch 1 (only job 3 late) → S2 prefetch 10 (5, 7, 9 wait) → S3 single active consumer (strict order, all wait) → S4 failover.
- **Kafka:** S5 slow job → S6 poison (LAG 4) → S7 DLQ (LAG 0).
- **Compare** (4 rows), **Checklist**, **More details**.

## Demo 07: replay
**Verdict:** Code OK, 2 minor simplifications. README 164 lines (~10 min) → ~85 (~4 min).

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Minor | `kafka/consume.php:5-6` | `[group]` argument is never used | `$group = 'replay.billing';` |
| 2 | Minor | `kafka/reprocess.php:14-30` | Metadata lookup + per-partition `$end` map for a 1-partition topic | `new TopicPartition('replay.events', 0, $ms)`, one `$high`. ~10 lines shorter. |
| 3 | Nit | `rabbitmq/resume.php:23`, `stream.php:29` | The same `x-stream-offset` read written twice | `function offset(AMQPMessage $m): int` in `stream.php` |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:15-19` | Intro table = Compare (`:18` = `:131` word for word) | P1 |
| 2 | Major | `:147-155` | Every Gotcha repeats an earlier line | Delete the section |
| 3 | Major | `:69` | ~110-word bullet on a 240K-message chunk experiment the learner won't see | "starts at the chunk, so you may get a few older messages" |
| 4 | Major | `:3-13` | 80-word protocol intro + 5 learn bullets that overlap the Checklist | 2-line Goal |
| 5 | Minor | `:54, :86, :104-109` | Long caveat bullets; the expected error and the `.000` rule sit away from the commands | Put them as `# fails: group is Stable` / `# .000 required` comments in the code block |
| 6 | Minor | `:92-94` | Doesn't say the reset runs in terminal 2 while consume keeps running | `# terminal 2 (cd kafka):` |
| 7 | Minor | `:128-137` | Compare has 8 rows with 30-40 word cells | ≤6 rows, ~8 words |
| 8 | Nit | `:87` | "file stores 4" right after the run showed `saved offset 3` | "Kafka 5 = our file's 4 for the same progress" |

### Suggested outline
- **Goal:** both brokers replay. Kafka stores each group's offset on the broker; a RabbitMQ stream read from PHP doesn't, so your consumer saves it (`billing.offset`).
- **RabbitMQ:** S1 resume from file → S2 no saved offset (`next` / `first`) → S3 by time/offset → S4 the int-as-timestamp trap.
- **Kafka:** S5 resume (broker offset) → S6 rewind (Stable error → Ctrl-C → `--to-datetime`) → S7 one-off `reprocess.php`.
- **Compare** (~5 rows), **Checklist**, **More details**.

## Demo 08: share-groups
**Verdict:** Code OK, one clever line and a few dead bits. README 159 lines (~9 min) → ~80 (~4 min). *(Edited during the review; re-check.)*

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Minor | `rabbitmq/publish.php:8` | `[, $count, $poison] = $argv + [1 => 9, 2 => 0];` is clever and forces casts | `$count = (int) ($argv[1] ?? 9); $poison = (int) ($argv[2] ?? 0);` |
| 2 | Nit | `reset.sh:11` | Can sit silently for up to 45 s; looks hung | `echo "waiting for share.workers members to leave (<=45 s)"` |
| 3 | Nit | `reset.sh:9` | Deleting `share.dlx` isn't needed (redeclared identically) | Drop |
| 4 | Nit | `rabbitmq/dead.php:9` | `basic_qos` not needed | Drop |
| 5 | Nit | `kafka/stuck-worker.sh:6-7` | `docker rm -f share.stuck` twice | `docker rm -f share.stuck >/dev/null 2>&1 \|\| true; [ "$1" = stop ] && { echo removed; exit; }` |
| 6 | Nit | `kafka/share-consumer.sh:4` | `--timeout-ms` never used | Drop it + its gotcha |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:97-102` | The ~40 s publish window isn't in the step (Fix first #4) | "Publish within ~40 s of `share.stuck is frozen`. If all 3 print at once, rerun." |
| 2 | Major | `:13-19` | Intro table = Compare | P1 |
| 3 | Major | `:58-59, :93, :106, :108` | Deep caveats you can't see in the run: heartbeats, `x-consumer-timeout` minimums, KIP-1191, the 2000-record lock window, RENEW | One line each; the rest goes to More details |
| 4 | Major | `:143-150` | Gotchas repeat code comments and earlier bullets | Delete; keep "non-TTY leftovers" in More details |
| 5 | Minor | `:53` | `php consume.php B & B=$!; sleep 2; kill -STOP $B` is shell trickery | `php consume.php B`, press **Ctrl-Z**; later `kill -9 %1` |
| 6 | Minor | `:92` | "Repeat with `--reject`" hides 3 steps | Spell them out |
| 7 | Minor | `:116` | The PHP check sits inside "ack ≠ delete" | Its own last step: "PHP can't consume" |
| 8 | Nit | `:46, :81` | Run-by-run splits ("3/3/3, 2/5/2, …") | "uneven split, all three get work" |

### Suggested outline
- **Goal:** Kafka 4.2+ share groups let many consumers share one partition with per-record ack and a delivery limit. Still missing: ack doesn't delete, no DLQ, a 30 s lock, no PHP client.
- **RabbitMQ:** S1 poison job → DLQ after 6 deliveries → S2 stuck (Ctrl-Z, Unacked 1) then crashed (`kill -9 %1`, immediate redelivery).
- **Kafka:** S3 classic group (2 idle) → S4 share group (3 members on partition 0) → S5 release (5 deliveries, archived, LAG 0) → S6 reject → S7 stuck worker (**publish within 40 s**) → S8 ack ≠ delete (`get-offsets` 0/23) → S9 PHP can't (`grep -ci share` → 0).
- **Compare** (~7 rows), **Checklist**, **More details**.

## Demo 09: log-compaction
**Verdict:** Code mostly clean (2 minor). README 161 lines (~7 min) → ~75 (~3 min). *(Edited during the review; findings reflect the 13:57 working tree.)*

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Minor | `kafka/read.php:16-23`, `kafka/rebuild.php:18-25` | The same EOF/timeout loop twice; `queryWatermarkOffsets` (`rebuild.php:12`) only feeds a guard | P7; delete `rebuild.php:12` |
| 2 | Minor | `kafka/publish.php:8-15,40-42` | New DR callback + `$failed` throw: 8 lines of plumbing. A missing topic already trips `flush()`. | Delete; drop idempotence |
| 3 | Nit | `kafka/rebuild.php:15,38` | `in %d ms` measures connect overhead; README:114 has to explain that away | Drop the timing + README:114's 2nd sentence |
| 4 | Nit | `reset.sh:5` | Deletes groups that are never created (readers use `assign()`) | Delete the line |
| 5 | Nit | `kafka/read.php:15,24` | `$offsets[]` only used for `count()` | `$count++` |
| 6 | Nit | `rabbitmq/publish.php:28` | `DELIVERY_MODE_PERSISTENT` does nothing on a stream | Drop |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:13-17` vs `:127-135` | 2 comparison tables with the same rows | Keep Compare only |
| 2 | Major | `:29-34, :77, :88, :92, :99-100, :114-117` | Config, timings, broker-log proof and tombstone mechanics fill each Act with 3-5 paragraphs | Command + output + 1 line per step |
| 3 | Major | `:144-151` | 4 of 6 gotchas repeat the Acts | Fold the new ones into More details |
| 4 | Minor | `:41` | Comment says user-3 ends with "deleted"; the code sends an empty body | "user-3 v1, then an empty body" |
| 5 | Minor | `:45` | Output listed grouped by key; the real output is in publish order | Paste real output |
| 6 | Minor | `:11, :119-123, :140, :159` | `__consumer_offsets` mentioned 4×, its command has no heading | "Step 5 (optional)" |
| 7 | Minor | `:48, :142` | LVC plugin history, tiered storage | More details |

### Suggested outline
- **Goal:** a compacted topic keeps the latest value per key; a RabbitMQ stream keeps every version.
- **RabbitMQ:** publish + read → 7 records (no key to compact by).
- **Kafka:** S1 publish + read (7) → S2 tick + poll (4, offset gaps) → S3 tick + poll (tombstone gone) → S4 `rebuild.php` → S5 optional `__consumer_offsets`.
- **Compare** (4 rows), **Checklist**, **More details** (4 configs, cleaner timing, log-cleaner proof, delete horizon, per-partition).

## Demo 10: durability
**Verdict:** Code works except the reset retry loop. README is **missing restart/verify steps** (blocker) and is long: 206 lines (~9 min) → ~100 (~4 min).

### Code
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `kafka-cli:6-8` + `reset.sh:12-13` | `until` never retries (grep's exit code). Verified. (Fix first #2) | `until ./kafka-cli kafka-topics --create … \| grep -q 'Created topic'; do sleep 1; done` |
| 2 | Minor | `kafka/produce.php:31-32` | 5-call metadata chain only prints the leader (`kill-leader` already does); crashes on a missing topic | Delete |
| 3 | Minor | `kafka/verify.php:20-27` | Re-implements `kafka_message()` | P7 |
| 4 | Nit | `reset.sh:22-23` | `php -r` JSON parser to print 3 lines | `./rabbit-cli rabbitmq-queues quorum_status durable.orders` |
| 5 | Nit | `compose.yaml:22` | `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS`: no group in this demo | Drop |
| 6 | Nit | `rabbitmq/classic.php:33-35` | `substr(…, 110)` truncates the error the step is meant to show | Print it in full |
| 7 | Nit | `kafka/verify.php:36-38`, `rabbitmq/verify.php:31-33` | "lost ids" sample is duplicated and never used | Drop |

### README
| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | **Blocker** | `:53-55, :69-72` | Missing `docker compose --profile kafka start` + `kafka/run verify.php <topic>`. Act 2 then runs on 2 brokers and times out instead of LOST 0. Verified. | Add both lines to both blocks, or `./reset.sh kafka` before Act 2 |
| 2 | Major | `:13-17, :170-187` | The same comparison 3× (intro, Compare, Talking points) | One 4-row Compare |
| 3 | Major | `:21, :49, :96-99, :134, :158-198` | Memory sizing, "5 of 5 runs", KIP-966, Erlang link detection, Vanlightly, 8 gotchas | More details |
| 4 | Minor | `:58` | Shows `leader kafka-3, isr 3,1,2`; `kill-leader:19` prints `… followers kafka-1 kafka-2` | Paste real output |
| 5 | Minor | `:89-90` | "Note the Leader", then hardcodes `kafka-2 kafka-3` | "stop the 2 non-leaders" |
| 6 | Minor | `:98, :130, :141, :153` | The `acks=1` result, "start it again", "publish again" are described in prose with no command | Add the commands |
| 7 | Minor | `:163` | `kafka-configs` needs the Kafka profile, which Setup already shut down | Move it into the Kafka section |
| 8 | Minor | `:21-30` | Setup mixes both profiles; the port map (5681-5683 = rabbit-1..3) is never stated | up/reset/down per section + port map line |
| 9 | Nit | `:111, :131, :142` | `rabbitmq/verify.php` drains the queue, so a 2nd run reports everything LOST | "run once" |

### Suggested outline
- **Goal:** what "acknowledged" means. Kafka: `acks` / `min.insync`. RabbitMQ quorum: majority + fsync.
- **Kafka** (`up --profile kafka`, `./reset.sh kafka`): S1 `acks=1` + stall → **start + verify** → LOST ~60k, 0 failures → S2 reset, `acks=all` → **start + verify** → LOST 0 → S3 2 of 3 down → `NOT_ENOUGH_REPLICAS` → S4 defaults, `down`.
- **RabbitMQ** (`up --profile rabbitmq`): S5 same chaos → LOST 0 → S6 leader kill (~1 s blip) → S7 2 of 3 down (confirms wait) → optional classic queue.
- **Compare** (4 rows), **Checklist**, **More details**.

## 99-final
**Verdict:** Matches 01-10 except one stale number and some drifted figures. 81 lines but ~1,400 words (~6 min) → ~3 min.

| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Major | `:19` | "rebuild state in ~500 ms"; 09 now says that's connect overhead | Drop |
| 2 | Minor | `:38` | Demo 10 "6 min, ~1.5 GB"; 10 says ~8 min, ~0.6 GB per profile | "8 min", "~0.6 GB" |
| 3 | Minor | `:13, :31, :42` | "~95 lines" vs 03's "retrier.php, 33 lines" | Pick one number |
| 4 | Minor | `:14, :32` | Regex pickup "7-11 s" / "~5-10 s"; 04 says 6-13 s | "6-13 s" |
| 5 | Minor | `:48-57` | "Proved wrong" misses demo 10's finding: a plain kill with `acks=1` lost 0 in 5/5; loss needs a follower stall | Add the row (most likely to flop on stage) |
| 6 | Minor | `:9-38` | Matrix cells are mini-paragraphs; the score table has 8 columns | ~8-word cells; merge "Dead air" into "Risk" |
| 7 | Nit | `:15, :29, :56` | Drifted figures (05: 276-314 s; 01: ~7 s; 02: 7-18 s) | Copy from each demo |

## Shared (lib, bin, compose, demo/README.md)
**Verdict:** Code OK, a few dead bits. README fine at 64 lines, but missing the host prerequisites.

| # | Severity | Where | Issue | Fix |
|---|---|---|---|---|
| 1 | Minor | `bin/kafka-php:6` | Silent auto-build (`-q >/dev/null`): the first `./run` looks hung for ~1 min | Delete; keep the explicit setup step |
| 2 | Minor | `README.md:23-30` | Missing host prerequisites | "Host: Docker, Composer, PHP 8.2+ with sockets, mbstring, pcntl, posix." |
| 3 | Nit | `lib/kafka.php:41-46` | `partitions()` returns the `'none (idle)'` sentinel that callers compare against | P11 |
| 4 | Nit | `lib/rabbitmq.php:10,12` | `RABBITMQ_HOST` is never set; the comment repeats `compose.yaml:8` | Hardcode `'localhost'`, drop the comment |
| 5 | Nit | `lib/Dockerfile:2-5` | `WORKDIR /app` is dead; rdkafka isn't pinned but 01 states exact versions | Delete; pin `rdkafka-6.0.5` or drop the versions |
| 6 | Nit | `bin/reset-topic:9` | Hand-written `1 2 … 15` | `$(seq 15)` |
| 7 | Nit | `compose.yaml:29-30,42` | EXTERNAL listener / host 9092 used by no demo | Drop unless you use host tools |
| 8 | Nit | `README.md:7-19, :33` | "Talk point" column is meaningless to a learner; the port workaround is troubleshooting | Drop the column; move the port workaround to Troubleshooting |
