* background: why sequentially read 10G in single thread? because of canal generator and streaming processing
* log operations: each log line = each operation(insert/delete/update property/update primary key)
* 4 actors: mmap reader, mediator, transformer pool, restore-computation worker, 3 blocking queues: mediator task, transform task, computation task
* record filed initialization by mmap reader, how will it influence the record scanner
* restore computation requires order -> dependency among transforming tasks
* how mediator coordinate transformers jobs from the polled mediator task
* how does record scanner work, why is it efficient, how to build log operation objects
* log operation storage, small object optimization
* restore computation logic(general/trick): data structure, corresponding actions
* general actions: (insert/delete/update property/update primary key)
* tricky actions: (insert/delete/update property), since update primary key can be viewed as delete and insert without property
* tricky point from update pk bug: statistics for useful chunks, if not useful, not submit to transformer pool
* history summary: 60s -> 40s, 40s -> 20s, 20s -> 14s, 14s -> 10s, 10s -> 8.9s(small object, pipeline, hashmap/hahset optimization, zero copy direct memory)
* tricky history summary: 8.9s -> 6.7s, 6.7s -> 4.8s(update key trick/skip transform task trick)
