## 算法流程设计

### 基本思路

* 重放算法分为两个阶段，第一个阶段：单线程顺序读取十个文件，重放出数据库中最后时候符合主键在查询范围内的记录；第二个阶段：遍历第一阶段的记录数组，针对每一条记录，插入主键为key并且`byte[]`为value的`ConcurrentSkipListMap`中， 为之后产生出对应的文件作准备。

* Server段在程序启动时候，开启一个线程监听Client连接请求；在最后执行完第二阶段计算时候，遍历有序的 `ConcurrentSkipListMap`， 产生出结果文件对应的 `byte[]`，并使用 java nio 的 `transferTo` 方式直接发送到Client并通过Client Direct Memory进行落盘。

### 流水线的设计

整个重放算法有关的类都放在 `server2` 文件夹下， 其中的类关系如下图所示。

![core pipeline logic](core_pipeline_logic.png)

图中有四种不同的actor：

* actor 1: MmapReader(主线程)， 负责顺序读取十个文件，按64MB为单位读取，若文件尾部不满64M就读取相应的大小, 读取之后对应的 `MappedByteBuffer` 会传入一个大小为1的 `BlockingQueue<FileTransformMediatorTask>`, 来让Mediator进行消费。因为阻塞队列的大小为1， 所以内存中最多只有三份 `MappedByteBuffer`(分别于主线程/Mediator线程/BlockingQueue中)， 总大小至多为192MB。

* actor 2: Mediator(单个Mediator线程)， 负责轮询 `BlockingQueue<FileTransformMediatorTask>`来获取任务， 一个任务中包含一个`MappedByteBuffer`和对应的Chunk大小。

轮询的逻辑如下代码所示：

```java
mediatorPool.execute(new Runnable() {
    @Override
    public void run() {
        while (true) {
            try {
                FileTransformMediatorTask fileTransformMediatorTask = mediatorTasks.take();
                if (fileTransformMediatorTask.isFinished)
                    break;
                fileTransformMediatorTask.transform();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
});
```

在最后读取完所有文件块的时候， 主线程会发送一个任务，通知 Mediator 可以结束了。该逻辑如下所示：

```java
try {
    mediatorTasks.put(new FileTransformMediatorTask());
} catch (InterruptedException e) {
    e.printStackTrace();
}
```

在收到任务后，Mediator负责分配， 保证每个Tokenizer and Parser处理的都是完整的块， 也就是说， 开始的index在`|mysql...`的`|`上， 结束的index在`\n`的后一个上。


* actor 3: Tokenizer and Parser for LogOperation(线程数为16的线程池)

* actor 4: Restore Computation Worker(单个重放计算线程)

* 任务二： `FileTransformMediatorTask` 这个对应的任务会被一直进行轮询的 Mediator单线程线程池消费。
