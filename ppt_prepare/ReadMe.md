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
