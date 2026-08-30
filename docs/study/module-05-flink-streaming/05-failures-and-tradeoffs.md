# Module 05 — Failures and tradeoffs

| Failure | Detection / impact | Current behavior | Recovery / improvement |
| --- | --- | --- | --- |
| lower LSN | version decision | state regression risk | reject | run replay proof |
| equal LSN, unknown identity | conflict decision | ambiguous mutation | reject conservatively | capture durable metadata |
| malformed message | parser | cannot type event | side-output quarantine | schema compatibility monitoring |
| checkpoint/sink failure | Flink metrics | duplicate/recovery risk | configured restart/checkpoint | validate real sink commits |

Flink is selected for stateful unbounded processing; Spark is better for bounded batch. Watermarks support event-time operations but do not replace source position. Parallelism can scale keys, but a hot key/state growth causes backpressure and requires measured partitioning/rescaling design.
