## Outline

1. 背景，题目分析和理解(input/output)
1. 核心思路overview: 第一阶段/第二阶段/网络传输和落盘
1. 流水线设计图
1. mmap reader
1. coordinator(mediator)
1. transformer pool
1. restore computation worker
1. 并行eval, zero-copy
1. restore computation不同版本（并行/通用/trick）
1. 工程价值: 背景契合度
1. 工程价值: 通用性(3 pages)
1. 工程价值: 通用性修改(针对`NonDeleteOperation`和`RecordScanner`)
1. 使用的trick和理论分析(2 pages)，不同时间可能用到的trick
1. 版本演进(通用版)
1. 版本演进(trick版)

## Content Thinking

1. background: why sequentially read 10G in single thread? because of canal generator and streaming processing, input - log operations: each log line = each operation(insert/delete/update property/update primary key), range e.g, (1000000, 8000000), schema and table(single/single), output - in-range records, sorted by key, finally persistent in client-side
1. 4 actors: mmap reader, mediator, transformer pool, restore-computation worker, 3 blocking queues: mediator task(control producer-consumer pace), transform task(no control), computation task(control producer-consumer pace)
1. record filed initialization by mmap reader, how will it influence the record scanner
1. how mediator coordinate transformers jobs(from the polled mediator task)
1. restore computation requires order -> dependency among transforming tasks(latter transform task needs to invoke `prevFuture.get()` to wait for previous one finishing putting output into computation task queue)
1. how does record scanner work, why is it efficient, how to build log operation objects
1. log operation storage, small object optimization
1. restore computation logic(general/trick): data structure, corresponding actions
1. general actions: (insert/delete/update property/update primary key)
1. tricky actions: (insert/delete/update property), since update primary key can be viewed as delete and insert without property
1. tricky point from update pk bug: statistics for useful chunks, if not useful, not submit to transformer pool
1. history summary: 60s -> 40s, 40s -> 20s, 20s -> 14s, 14s -> 10s, 10s -> 8.9s(small object, pipeline, hashmap/hahset optimization, zero copy direct memory)
1. tricky history summary: 8.9s -> 6.7s, 6.7s -> 4.8s(update key trick/skip transform task trick)
