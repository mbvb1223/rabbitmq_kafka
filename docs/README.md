# RabbitMQ vs Kafka — technical sharing session prep

| File | What's in it |
|---|---|
| [01-source-summary.md](01-source-summary.md) | Full structured breakdown of <https://www.rabbitmq.com/docs/compare/kafka> — every section, table, and technical claim |
| [02-related-research.md](02-related-research.md) | Bias audit of that page, independently verified facts, the benchmark caveats, and the **PHP ecosystem angle** (php-amqplib vs php-rdkafka, no official PHP stream client, the parallelism ceiling) |
| [03-topic-proposal.md](03-topic-proposal.md) | **← the deliverable.** Proposed title, 5 main points, 2 PHP demos, 16-slide skeleton, takeaway slide, Q&A risks, open questions |

**Status:** proposal drafted, awaiting approval/edits. Versions pinned to Kafka 4.3 / RabbitMQ 4.3.

**Headline:** the "Kafka = streaming, RabbitMQ = queueing" rule has been eroding since RabbitMQ shipped streams in 3.9 (2021), and died in Feb 2026 when Kafka 4.2 took share groups (KIP-932) GA. The remaining difference is *how much the broker knows about each message*, plus, for a PHP shop, client maturity and the process model.
