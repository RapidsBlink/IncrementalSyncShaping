> 写在前面
1. 赛题答疑联系人(可通过旺旺联系)：万少
2. 开始Coding前请仔细阅读以下内容
3. 外部赛复赛旺旺群：1251809708
4. 该文档根据实际情况会进行更新，选手每次写代码前建议pull下该样例工程，查看README文件的变化


# ================================================== 赛题规则 ============================================================

# 1. 赛题描述
题目主要解决的是数据同步领域范畴：实时增量同步，主要的技术挑战为模拟数据库的主备复制，提供"高效"的实时同步能力。即给定一批固定的增量数据变更信息，程序需要收集增量变更信息，并进行一定的数据重放计算，然后将最终结果输出到给定的目标文件中。增量数据的变更信息为了简化处理，会给出明文的数据，主要包含数据库的insert/update/delete三种类型的数据。具体的增量数据变更信息的数据格式见环境描述部分。数据重放主要是指模拟数据库的insert/update/delete语义，允许使用一些中间过程的存储。

# 2. 环境描述：
## 2.1 整体格式
有2台机器，简称A、B机器，A机器里会保存增量数据文件并且在固定的目录下提供10个文本文件，每个文本文件大概为1GB左右。每个文件有若干条变更信息。每条变更信息的记录由多列构成。文本中每行记录的格式为:

|     binaryId   | timestamp   |schema|table|变更类型|列信息|变更前列值| 变更后列值|列信息|列值|...|
| ------------- |-------------| -------------| -------------| -------------| ------------| -------------| -------------|------------| -------------|-------------| 


## 2.2 格式解释：

a. binaryId： 一个唯一的字符串编号,例子:000001:106

b. timestamp
  * 数据变更发生的时间戳,毫秒精度,例子:1489133349000
  
c. schema/table
  * 数据变更对应的库名和表名
  
d. 变更类型(主要分为I/U/D)
  * I代表insert, U代表update, D代表delete
  
d. 列信息
  * 列信息主要格式为，列名:类型:是否主键
  * 类型主要分为1和2
  * 1代表为数字类型，数字类型范围为0<= x <= 2^64-1
  * 2代表为字符串类型，0<= len <= 65536
  * 是否主键:0或者1 (0代表否，1代表是) 
  * 例1： id:1:1 代表列名为id,类型为数字,是主键
  * 例2： name:2:0 代表列名为name,类型为字符串,非主键
  

e. 列值
 * 主要分为变更前和变更后,NULL代表物理值为NULL(空值),(可不考虑字符串本身为"NULL"的特殊情况)
 * insert变更,只有变更后列值,其变更前列值为<NULL>,会包含所有的列信息
 * upadate变更,都会有变更前和后的列值,会包含主键和发生变更列的信息(未发生变更过的列不会给出,不是全列信息)
 * delete变革,只有变更前列值,会包含所有的列信息

## 2.3 格式例子
 

实际例子:
 * 000001:106|1489133349000|test|user|I|id:1:1|NULL|102|name:2:0|NULL|ljh|score:1:0|<NULL>|98|
 * 000001:106|1489133349000|test|user|U|id:1:1|102|102|score:1:0|98|95|  //执行了变更update score=95 where id=102


## 2.4 注意事项

1. 每一行代表一条变更数据,注意几个数据变更场景
2. 表的主键值也可能会发生update变更
3. 表的一行记录也可能发生先delete后insert
3. 整个数据变更内容是从零条记录开始构建的，即为任意一条行记录的update之前一定会有对应行的insert语句，delete之前也一定有一条insert,不考虑DDL变更产生列类型变化，也不需要考虑其他异常情况


#3. 程序要求
## 3.1 程序实现

分为server和client两部分：
1. server会传入对应的文本的绝对路径地址，启动后需要开启网络服务，等待client建立链接之后，需要使用push机制主动开始推送数据到client
2. client在启动时会传入server机器地址，主动和server建立链接之后，等待server推送数据，接收到数据后进行数据重放处理，等收完所有数据后，生成最终的结果到给定的目标文件名.

## 3.2 程序校验
结果校验，会传入一批数据(格式为:schema+table+pk)，程序要按顺序返回每个主键对应记录的所有列的最终值.时间计算，会计算从程序启动到最后返回结果的时间差值

## 3.3 使用语言
JAVA

# 4. 排名规则

在结果校验100%正确的前提下，按照总耗时进行排名，耗时越少排名越靠前，时间精确到毫秒。

# ================================ 如何模拟赛题数据  ===================================

1. 从阿里云代码仓库下载[canal(复赛模拟数据版)](https://code.aliyun.com/wanshao/canal.git)。canal是一款开源的mysql实时数据解析工具，在github的地址为[canal(开源版)](https://github.com/alibaba/canal)
2. 自己安装好mysql，并且建好相关的schema和table
3. 根据[QuickStart](https://github.com/alibaba/canal/wiki/QuickStart)启动canal server
4. 启动[canal(复赛模拟数据版)](https://code.aliyun.com/wanshao/canal.git)中的SimpleCanalClientTest这个客户端程序(建议直接在IDE中启动)
5. 对mysql数据库进行一些DML操作，触发binlog变更，默认在/Users/wanshao/work/canal_data/canal.txt路径下会生成符合赛题要求的变更数据

PS：
1. 修改默认路径请更改AbstractCanalClientTest.storeChangeToDisk()方法
2. canal中转化数据的代码请看：analyseEntryAndFlushToFile()方法
3. canal中解析生成赛题数据时，现在只有int类型换解析成"数字类型"，其他的类型都会解析成"字符串类型"


# ========================= 评测程序如何工作 ============================================
概要说明：评测程序也分为Server和Client，请留意

1. 从天池拉取选手git地址
2. 对选手代码在评测程序的server端进行编译打包
3. 将选手代码从评测程序的server端拷贝到评测程序的client端
4. 在评测程序server端记录开始时间startTime
5. 通过shell脚本在评测程序的server端启动选手的server程序
6. server端评测程序通过HTTP请求通知client端评测程序去启动选手的client端程序(会传给client端一个serverIp)

```
# 启动选手server，并且传入相关
java $JAVA_OPS -cp $jarPath com.alibaba.middleware.race.sync.Server $schema $tableName $start $end
# 启动选手client
java $JAVA_OPS -cp $jarPath com.alibaba.middleware.race.sync.Client
```

7. client端评测程序等待client端程序运行结束(退出JVM)后，到指定目录拿选手的最终结果和标准结果进行比对，并且将结果(结果正确、超时、结果错误)返回给server端评测程序
8. server端评测程序得到结果，如果结果有效，记录结束时间endTime。如果结果无效则直接将相关错误信息之间返回给天池系统。
10. server端评测程序强制kill选手的server端进程
11. server端评测程序将最终的间隔finalTime=(endTime-starttime)上报给天池系统，由天池进行排名


注意点：
1. 选手的server端程序由server端的评测程序来kill
2. client端的评测程序需要选手自己控制在得到最终结果后停止，否则会有超时问题

# ============================= 如何获取评测日志 ===================================
1. 超时时间： server端不做超时处理，client端超时时间为10分钟
2. 日志处理：
    - 请将日志写入指定的日志目录：/home/admin/logs/${teamCode}/，这里的teamCode请替换成自己的唯一teamCode，此外请不要透露自己的teamCode给别人哦。
    - 日志的命名按照如下命名：${test.role}_${teamCode}_custom_WARN.log和${test.role}_${teamCode}_custom_INFO.log。
3. 如何获取自己运行的日志：
    - 选手每次提交的程序运行的gc日志以及符合上面命名规范的日志，评测程序才会将其反馈给选手。
    - 选手可以通过地址：http://middle2017.oss-cn-shanghai.aliyuncs.com/${teamCode}/${logName} 这样的形式获取自己的日志
    - ${teamCode}是选手的唯一识别码之一，${logName}的名称可以为gc.log、{test.role}_${teamCode}_custom_INFO.log、{test.role}_${teamCode}_custom_WARN.log和{test.role}_${teamCode}_custom_ERROR.log三者之一.
4. 如何获取评测日志： 
    - 选手可以通过地址：http://middle2017.oss-cn-shanghai.aliyuncs.com/${teamCode}/assessment_${teamCode}_INFO.log 这样的形式获取自己的评测日志
    - 评测日志中${teamCode}替换成自己的teamCode



# ================================= 如何使用Demo ================================
Demo基于netty实现了简单的客户端和服务端程序。
1. Server: 负责接收评测系统给其的输入(通过args参数)，并且将解析好的数据交给Client。
2. Client: 启动后根据评测系统给其的serverIp来启动，启动后接受Server的信息，并且将最终结果写入到指定结果文件目录

```
Server端Program arguments示例（2个参数）：
middleware student 100 200

Client端Program arguments示例：
127.0.0.1

```

3. Constants: 比赛时候凡是需要写文件的地方，必须写到指定目录，对相关目录进行了定义。写出的目录路径都包含${teamcode}，千万不能遗漏。
teamcode是识别选手的唯一标示，评测程序会从选手teamcode相关目录下读取选手的结果文件，并且清理垃圾文件。

```
    // 赛题数据
    String DATA_HOME = "/home/admin/canal_data";
    // 结果文件目录(client端会用到)
    String RESULT_HOME = "/home/admin/sync_results/${teamcode}";
    // 中间结果目录（client和server都会用到）
    String MIDDLE_HOME = "/home/admin/middle/${teamcode}";
    //结果文件的命名
    //String RESULT_FILE_NAME = "Result.rs";
    
    PS： 中间结果一定要写到指定目录，否则评测程序将不会对其清理。写到非法目录，然后在下次评测的时候直接读取中间结果来提升成绩的视为违规。请注意！
```

4. Demo仅做演示用，选手可以自由选择其他通信框架，自己按照自己的方式来处理通信。



必读的注意点：
1. 写出中间结果文件一定要写指定目录
2. 写出结果文件一定要用指定的名字
3. 结果文件要在Client端写到指定目录
4. Client和Server的类名必须是"Client"和"Server"，否则评测程序无法正常启动选手的程序
5. 评测程序给server的参数，第一个参数是schema名字，第二个参数是table名字，第三个参数和第四个参数表征查询的主键范围。具体可以查看Demo
6. 构建工程必须保证构件名字为sync，最后得到的jar为sync-1.0.jar，建议使用Demo里面的assembly.xml，用mvn clean assembly:assembly -Dmaven.test.skip=true命令打包。
7. 结果文件的格式可以使用SQL:select * into outfile 'student.txt' from student来获得。默认每一列都是以tab分隔，每一行都以'\n'来换行
8. 变更信息的10个数据文件命名为： 1.txt、2.txt、3.txt、4.txt、5.txt、6.txt、7.txt、8.txt、9.txt、10.txt


结果文件格式例子如下，每列分别代表ID、名称、城市、性别

```
1	李雷   杭州  男
2   韩梅梅 北京  女
```







# =========================================== FAQ =========================================

> 环境相关

1. JDK采用什么版本？

```
java version "1.7.0_80"
Java(TM) SE Runtime Environment (build 1.7.0_80-b15)
Java HotSpot(TM) 64-Bit Server VM (build 24.80-b11, mixed mode)
```

2. 磁盘信息？

```
$sudo fdisk -l /dev/sda5

Disk /dev/sda5: 424.0 GB, 423999045632 bytes
255 heads, 63 sectors/track, 51548 cylinders
Units = cylinders of 16065 * 512 = 8225280 bytes
Sector size (logical/physical): 512 bytes / 4096 bytes
I/O size (minimum/optimal): 4096 bytes / 4096 bytes
Disk identifier: 0x00000000
```

```
# 写性能
$sudo  time dd if=/dev/zero of=/data/test  bs=8k count=1000000
1000000+0 records in
1000000+0 records out
8192000000 bytes (8.2 GB) copied, 18.6796 s, 439 MB/s
0.19user 15.03system 0:18.80elapsed 80%CPU (0avgtext+0avgdata 3760maxresident)k
1152inputs+16000000outputs (1major+360minor)pagefaults 0swaps
# 读性能
$sudo time dd if=/dev/sda5  of=/dev/null bs=8k count=1000000
1000000+0 records in
1000000+0 records out
8192000000 bytes (8.2 GB) copied, 25.6534 s, 319 MB/s
0.13user 7.93system 0:25.65elapsed 31%CPU (0avgtext+0avgdata 3792maxresident)k
16000320inputs+0outputs (1major+362minor)pagefaults 0swaps
```

3. CPU信息? 答：24核 2.2GHz

4. 最大文件打开数是多少？

```
$ulimit -a
core file size          (blocks, -c) 0
data seg size           (kbytes, -d) unlimited
scheduling priority             (-e) 0
file size               (blocks, -f) unlimited
pending signals                 (-i) 386774
max locked memory       (kbytes, -l) unlimited
max memory size         (kbytes, -m) unlimited
open files                      (-n) 655350
pipe size            (512 bytes, -p) 8
POSIX message queues     (bytes, -q) 819200
real-time priority              (-r) 0
stack size              (kbytes, -s) 10240
cpu time               (seconds, -t) unlimited
max user processes              (-u) 386774
virtual memory          (kbytes, -v) unlimited
file locks                      (-x) unlimited
```

5. 是否开启超线程？答：开启


6. 启动选手的Client和Server采用怎样的JVM配置？

```
-XX:InitialHeapSize=3221225472 -XX:MaxDirectMemorySize=209715200 -XX:MaxHeapSize=3221225472 -XX:MaxNewSize=1073741824 -XX:MaxTenuringThreshold=6 -XX:NewSize=1073741824 -XX:OldPLABSize=16 -XX:OldSize=2147483648 -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
```


> 规则相关

1. 哪些属于违规行为，会取消成绩甚至参赛资格? 

```
选手依靠自己的算法和程序，通过程序的运算得到正确答案一般都是不会违规的。
通过一些hack的方式而不是依靠程序算法本身来获取成绩，视为无效。
选手如果对某些行为是否违规不确定，可以联系我。当然相关内容我是不会透露给别的选手的。
```

2. 是否可以使用别的语言或者在JAVA中使用shell脚本？

```
不可以。之所以不支持别的语言主要原因是：
1. JAVA属于比较好上手的语言，其语法也能迅速掌握，如果能用其他语言实现自己的算法，换成JAVA也是比较容易的
2. 中间件部门内大量使用JAVA，从用人角度来说，也希望通过比赛挖掘相关的人才
3. 开发评测工具的人手有限，如果多语言的话，还要考虑C++、C、python、go、ruby等语言，代价较高
4. 限定使用一种语言，相对来说更加公平，屏蔽了不同语言之间的影响
```

3. 可以随意使用三方库吗？

```
可以。选手可以随意选择自己所需要的三方库
```

4. 不写到指定目录是否可以？

```
不可以！！评测程序会对指定目录下的选手中间结果清理。选手将中间结果写入非法目录，评测程序将不会及时对其进行清理。下次选手运行时可能会
```

5. 可以使用堆外内存吗？

```
可以，不过只能使用200MB，这个已通过JVM参数指定
```

6. 数据文件是按照时间排序的吗？

```
是的，1.txt中的时间最早，10.txt文件中的变更信息时间最晚。同一个文件内也是最前面的行时间最早
```


