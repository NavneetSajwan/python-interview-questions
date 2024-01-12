## what is spark
    spark is a distributed big data processing engine. it can process batch as well as realtime data,spark works of the conecpt in memory computation that'why it is 100 time faster than the hadoop map-reduce

## what is narrow and wide transformation in spark
Narrow transformations transform data without any shuffle involved 
exm: result of map(), filter(),union() 

if we need multiple partition to perform operation is called wide transformation

Wider Transformation exms:
groupByKey() , aggregateByKey() , aggregate() , join() , repartition()

## schema validation in deltalake
we are storing table schema in dynamodb if the table is not created then create table using delta and store the schema in dyanamodb.then next time we will load the data from source with applying this schema.

## what is different cluster mode in spark
1.standalone
2.apache mesos
3.hadoop yarn
4.kubernetes

## Spark Deploy Modes – Client vs Cluster Explained
<!-- https://sparkbyexamples.com/spark/spark-deploy-modes-client-vs-cluster/ -->



## why we are not using upsert
because we have three duplicate id in bronze table and we have apply upsert thet if id match then update all so it will do three times,but if we remove duplicate during loading raw table and then we can directly delete and append data at once

## how to remove duplicate senerio 

<!-- https://docs.google.com/document/d/1XyYbt5joaOzHvhd-cnOrh-v4dMOrRXlq5p8Eli_Ha9c/edit -->

we are first we perform row_num function on data partition by id and order by timestamp desc accroding to latest timestamp we have row number one then select it and check that this id is exist in destination then first drop it and then dump it


if primary_key:
columns = get columns from (destination table if it exists) else from the schema management module
CREATE OR REPLACE TABLE  {pipeline}_{task}_unique_load SELECT {columns}  
FROM ( SELECT *, row_number() over (PARTITION BY {','.join(primary_key)}  ORDER BY {timestamp_column} DESC) 
AS udp_internal_row_number 
FROM {pipeline}_{task}_temp_load ') 
AS ROWS 
WHERE udp_internal_row_number = 1
remove duplicates from destination table(supports composite keys)



## five transformation
1. map()
2. flatmap()
3. filter()
4. mapPartitions()
5. union()
6. reduceBykey()

## five action in spark
1. count()
2. aggregate()
3. take()
4. top()
5. collect()
6. reduceByValue()
7. reduce


## how does spark submit work

Using spark-submit, the user submits an application,in this section we have driver program that execute user code into task and schedule those task into executor ,we have cluster manager that are responsible for resource allocation and launch the executor ,once the executor launched all the spark jobs are divided into task and execute in this area and executor also reponsible of providing memory for rdd,dataframe and dataset which cached by user.

## how to join small dataframe with big dataframe(imp).
<!-- https://stackoverflow.com/questions/32435263/dataframe-join-optimization-broadcast-hash-join -->
For better performance, you can use broadcast hash join
The broadcasted dataframe will be distributed in all the partition which increases the performance in joining.

1. PySpark BROADCAST JOIN can be used for joining the PySpark data frame one with smaller data and the other with the bigger one.
2. PySpark BROADCAST JOIN avoids the data shuffling over the drivers.
3. PySpark BROADCAST JOIN is a cost-efficient model that can be used.
4. PySpark BROADCAST JOIN is faster than shuffle join.


## what is dag in spark
dag is nothing but a (directed acyclic graph)  which convert logical execution plan into physical execution plan
DAG in Apache Spark is a set of Vertices and Edges, where vertices represent the RDDs and the edges represent the Operation to be applied on RDD.


## lineage graph in spark
In Spark, Lineage Graph is a dependencies graph in between existing RDD and new RDD. It means that all the dependencies between the RDD will be recorded in a graph, rather than the original 

## what is default partitioner class used by spark
HashPartitioner is the default partitioner used by Spark. 
RangePartitioner will distribute data across partitions based on a specific range. 
The RangePartitioner will use a column (for a dataframe) that will be used as partition key.
defult partition=200

## what is lazy evaluation:
lazy evaluation means an transformation not perform untill and an action not perform means
 It will continue to do nothing, until you ask it for the final answer.

## Spark Performance Tuning – Best Guidelines & Practices
few ways to increase spark job optimization
    df.unpersists()
    Use DataFrame/Dataset over RDD.
    Use coalesce() over repartition()
    Use mapPartitions() over map()
    Use Serialized data format's.
    Avoid UDF's (User Defined Functions)
    Caching data in memory.
    Reduce expensive Shuffle operations.
    Disable DEBUG & INFO Logging.

## Note(Spark Performance Tuning): https://sparkbyexamples.com/spark/spark-performance-tuning/

## diff between cache and checkponiting
after the application terminate the cache is cleared or file destroyed 
The checkpoint file won't be deleted even after the Spark application terminated.


## what is checkpointing

checkpointing is the processs spark use to store the metadata in a dir in case of spark job failure then it can directly process the data where it  is failed
 There are mainly two types of checkpoint one is Metadata checkpoint and another one is Data checkpoint.

1.dataset checkpointing 
2.metadata checkpointing


## what is structure streaming and types of structure streaming
Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can express your streaming computation the same way you would express a batch computation on static data.

## why dataframe is faster than rdd
RDD – RDD API is slower to perform simple grouping and aggregation operations. DataFrame – DataFrame API is very easy to use. It is faster for exploratory analysis,
because rdd not

## Spark – Difference between Cache and Persist?
```
Using cache() and persist() methods, Spark provides an optimization mechanism to store the intermediate computation of an RDD, DataFrame, and Dataset so they can be reused in subsequent actions(reusing the RDD, Dataframe, and Dataset computation result’s).

Both caching and persisting are used to save the Spark RDD, Dataframe, and Dataset’s. But, the difference is, RDD cache() method default saves it to memory (MEMORY_ONLY) whereas persist() method is used to store it to the user-defined storage level.
```
## PySpark Broadcast Variables
https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/ 

```
In PySpark RDD and DataFrame, Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks. Instead of sending this data along with every task, PySpark distributes broadcast variables to the workers using efficient broadcast algorithms to reduce communication costs.

follow->interview_preprations/spark_tutorial/Spark_RDD_Tutorial
```
## diff between  Accumulator and Broadcast Variables
```
An accumulator is also a variable that is broadcasted to the worker nodes.
The key difference between a broadcast variable and an accumulator is that while 
the broadcast variable is read-only, the accumulator can be added to 

Accumulator can be used to implement counters (as in MapReduce) or sums. 
Spark natively supports accumulators of numeric types, and programmers can add support for new types.
```

## spark executor(imp)
https://spark.apache.org/docs/latest/cluster-overview.html


## find second higest salary
    SELECT SALARY,NAME FROM EMPLOYEE ORDER BY SALARY DESC LIMIT 1 OFFSET 1 (n-1) --Second largest

## interview links:
    apache spark interview question
    
    https://www.simplilearn.com/top-apache-spark-interview-questions-and-answers-article

    https://sparkbyexamples.com/pyspark/pyspark-aggregate-functions/

## spark example run
    https://sparkbyexamples.com/spark/spark-how-to-kill-running-application/

## rdd of spark
    https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html

## increase and decrease no of partitions and parallelism
    spark.conf.set("spark.sql.shuffle.partitions", "1000")
    spark.conf.set("spark.default.parallelism", "1000")

    When you run a PySpark RDD, DataFrame applications that have the Broadcast variables defined and used, PySpark does the following.

    PySpark breaks the job into stages that have distributed shuffling and actions are executed with in the stage.
    Later Stages are also broken into tasks
    Spark broadcasts the common data (reusable) needed by tasks within each stage.
    The broadcasted data is cache in serialized format and deserialized before executing each task.
    You should be creating and using broadcast variables for data that shared across multiple stages and tasks.

## show full text in df
    use df.show(truncate=False)


## spark coding links
    https://sparkbyexamples.com/spark/spark-web-ui-understanding/

    https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html


## what is job scheduling in spark
    By default, Spark's scheduler runs jobs in FIFO fashion. Each job is divided into “stages” (e.g. map and reduce phases), and the first job gets priority on all available resources while its stages have tasks to launch, then the second job gets priority, etc.


## [spark executor(imp) and spark archeticture](https://spark.apache.org/docs/latest/cluster-overview.html)

https://www.youtube.com/watch?v=JSvzGJC8vjQ
```
spark core component :spark ML-LIB,spark-GraphX,spark-streaming,spark-sql

spark executor : standalone(local),kubenetes ,hadoop yarn ,apche messos

    standalone : this is very simple cluster manager include with spark

    hadoop-yarn : the resource manager of hadoop 2 which stand for yet another negotiator

    apache-mesos : general cluster manager that can run hadoop mapreduce and service as well

```
 ![Archeticture](./image/cluster-overview.png)

```
spark Archeticture define into three part
1.driver program/sparkSession
2.cluster manager
3.worker node (Executor + task)

1.driver program /spark session => 
    this is the main entry point of spark application. the main method of program runs in driver
    driver create sparkSession/ sparkContext
    driver perform transformation and action

    main role of driver program is that
    1. convert user program into task
    2.scheduling task in the executor

2.cluster manager =>
    cluster manager are responsibe for resource allocation anmd launch executor.
    cluster manager is pluggable component of spark
    cluster manager can dynamically adjust resouce used by the spark application

3.Spark Executor:
    the individual task given by the spark job are runs in the spark executor
    Executor Launch once in the begning of the spark application and they run for the entire life cycle of an application

    the two main role play the executor
    1. runs the task that makes up the spark application and return the result to the driver
    2. proivde in memory storage for RDD/DataFrame/DataSets that are cached by the users

```

## [how to optimize spark job](https://medium.com/datakaresolutions/key-factors-to-consider-when-optimizing-spark-jobs-72b1a0dc22bf)

    1.repartitions (repartitions split and suffle the data accorss all the node in a cluster)
    2.broadcating
    3.Avoid UDF and UDAF (please try to avoid user define function and use spark optimize function)

    4.Dynamic allocation
        According to the workloads it is possible to scale up and scale down the number of executors which is known as dynamic allocation.

        $ spark.dynamicAllocation.enabled //Set the value to true to enable dynamic allocation.
    
    5.Parallelism
        In spark higher level APIs like dataframe and datasets use the following parameters to determine the partition of a file.

        $ spark.sql.files.maxPartitionBytes //Specify maximum partition size by default 128mb


## [what is dataset in spark](https://www.educba.com/spark-dataset/)
    Dataset is a new interface added in Spark 1.6 that provides the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL's optimized execution engine.

    the basic difference between dataframe and dataset is 
    Type Safety: Dataset provides compile-time type safety. It means that the application’s syntax and analysis errors will be checked at compile time before it runs.
.   Python does not have the support for the Dataset API.


## [what is parquet and why it is used and how this is works internelly](https://www.youtube.com/watch?v=0quJ9gXO6Dg)

```
https://www.upsolver.com/blog/apache-parquet-why-use
    what is row based and column based 
    let's take an example we have a file in which have 5 column and 100 record then what happen
    in csv make 100 files in which one full information of a row but other hand parquet store data into 5 columns so the reterival of only speific column data much fatser then raw based
```

## pyspark join
    pyspark dataframe support sql type join like inner,outer,left,right
    df.join(df2,df.id==df2.id,'inner')

## mapPartitions() 
is used to provide heavy initialization for each partition instead of applying 
to all elements this is the main difference

## data is skewed to remove by using repartitions
Use option maxRecordsPerFile if you want to control the number of records for each partition. This is particularly helpful when your data is skewed (Having some partitions with very low records and other partitions with high number of records).

## spark suffling and partitions
https://sparkbyexamples.com/spark/spark-shuffle-partitions/ <br>

Based on your dataset size, number of cores, and memory, Spark shuffling can benefit or harm your jobs. When you dealing with less amount of data, you should typically reduce the shuffle partitions otherwise you will end up with many partitioned files with a fewer number of records in each partition. which results in running many tasks with lesser data to process.

On other hand, when you have too much of data and having less number of partitions results in fewer longer running tasks and some times you may also get out of memory error.

Getting a right size of the shuffle partition is always tricky and takes many runs with different value to achieve the optimized number. This is one of the key property to look for when you have performance issues on Spark jobs.


## [diff between ETL AND ELT](https://www.guru99.com/etl-vs-elt.html)

diff.
Transformations are done in ETL server/staging area. but in ELT Transformations are performed in the target system 

ETL model used for on-premises, relational and structured data.Used in scalable cloud infrastructure which supports structured, unstructured data sources. 


## what is bucket in spark
https://sparkbyexamples.com/apache-hive/hive-bucketing-explained-with-examples/#:~:text=In%20general%2C%20the%20bucket%20number,type%20of%20the%20bucketing%20column.

Bucketing is a technique in both Spark and Hive used to optimize the performance of the task. In bucketing buckets (clustering columns) determine data partitioning and prevent data shuffle. Based on the value of one or more bucketing columns, the data is allocated to a predefined number of buckets

## how to decide number of buckets in spark

expression hash_function(bucketing_column) mod num_buckets

## when to use cache 
If you're executing multiple actions on the same DataFrame then cache it. Every time the following line is executed (in this case 3 times), spark reads the Parquet file, and executes the query. Now, Spark will read the Parquet, execute the query only once and then cache it

## when to use persists
It stores the data that is stored at a different storage level the levels being MEMORY and DISK.

## buckting vs partitions (buckting avoid data suffle)
Bucketing is similar to partitioning, but partitioning creates a directory for each partition, whereas bucketing distributes data across a fixed number of buckets by a hash on the bucket value. Tables can be bucketed on more than one value and bucketing can be used with or without partitioning

## hwo to handle duplicate value while loading data
    we are using rank function if same value then rank woud be same but the timestamp would be diffrent so that we will take latest timestamp



DELETE FROM deltalake.product.destinationTable
                    WHERE primaryKey IN
                    SELECT primaryKey FROM deltalake.source.sourceTable
                    WHERE CAST(timestampColumn as timestamp) > CAST(lastTimestamp as timestamp))

    schema -> floor plan
    database -> house
    table -> room

## what is spark core and executor (cores works inside executor)
 The cores property controls the number of concurrent tasks an executor can run. --executor-cores 5 means that each executor can run a maximum of five tasks at the same time. 

Note : spark core is like a cpu i3,i5 processor 

## exm of spark submits
    --num-executor => just like container or jvm
    --executor-memory =>executor have it's own memory to execute task
    --executor-cores =>core ar like cpu that control no of concurrent tasks an executor
    --driver-memory => when you do spark-submit then the driver will created and driver communicate with master so if it is  spark then master or if it is yarn then it communicate with application master


## (what is the difference between executor and core in spark)[!https://www.youtube.com/watch?v=PP7r_L-HB50]
 Number of executors is the number of distinct yarn containers or we can say it is a container where task are executed (think processes/JVMs) that will execute your application. (execuotr is like a vm)

## what is spark job
 Jobs are the main function that has to be done and is submitted to Spark.

##  driver memory
The - -driver-memory flag controls the amount of memory to allocate for a driver, which is 1GB by default and should be increased in case you call a collect() or take(N) action on a large RDD inside your application.

## What are Stages in Spark?

A stage is nothing but a step in a physical execution plan. or we can say that each job are divided into task which are totally dependent to each other

Stages in Apache spark have two categories
1. ShuffleMapStage in Spark
2. ResultStage in Spark

## what is serialization in spark(keyro serilization)
A serialization framework helps you convert objects into a stream of
 bytes and vice versa in new computing environment.

## hadoop map-reduce
MapReduce is a programming model or pattern within the Hadoop framework that is used to access big data stored in the Hadoop File System (HDFS)

## diff between map and flatmap is
simple: flatmap have the heavy initilization of each partition instead of apllying each value,
or if we apply flat map then we will get flatten list but in map we will get list of list

text_file=sc.textFile("rr.txt")

for unique word count then use flatmap like exm:
<!-- =-============================================= -->
=======>text_file.map(lambda x:x.split(" ")).take(2)
o/p like below 
<!-- ============================================ -->
[['This',
  'document',
  'details',
  'out',
  'the',
  'proposal',
  'for',
  'the',
  'user',
  'experience',
  'for',
  'UDP',
  '-'],
 ['']]

<!-- ==================now in flatmap data is look like========================== -->
text_file.flatMap(lambda x:x.split(" ")).take(10)

<!-- not list of list -->
['This',
 'document',
 'details',
 'out',
 'the',
 'proposal',
 'for',
 'the',
 'user',
 'experience']
<!-- ============================================================================= -->


## how to perform unique word count in file
1.first load the text file
2.then get each word  using flatmap()
3.then grouping value by 1 using map like ("ddd",1)
4.aplly reduceBykey

    rdd2=rdd.flatMap(lambda x: x.split(" "))
    for element in rdd2.collect():
        print(element)
    #map
    rdd3=rdd2.map(lambda x: (x,1))
    for element in rdd3.collect():
        print(element)
    #reduceByKey
    rdd4=rdd3.reduceByKey(lambda a,b: a+b)
    for element in rdd4.collect():
        print(element)
    #map


## how to search word in a file
lines = sc.textFile(“hdfs://Hadoop/user/test_file.txt”);

def isFound(line):

if line.find(“my_keyword”) > -1

return 1

return 0

foundBits = lines.map(isFound);

sum = foundBits.reduce(sum);

if sum > 0:

print “Found”

else:

print “Not Found”;



## bottleneck problem in spark

## What does Spark UI identify in bottlenecks?
Identifying Performance Bottlenecks
Resource Planning (Executors, Core, and Memory)
Degree of Parallelism – Partition Tuning (Avoid Small Partition Problems)
Straggler Tasks (Long Running Tasks)

## how to count empty line in a file
text_file=sc.textFile("rr.txt")
text_file=text_file.filter(lambda x :len(x)==0)
total=text_file.countByValue()

## what is drver
driver is the main method which will triger the spark context/spark session

## [when does spark go out of memory issue.](https://www.youtube.com/watch?v=FdT5o7M35kU)


1. driver memory issue
    1. collect()(you have three bigger file and three executor process the data after you want to apply collect() then you will face up out of memory error you can use take)
    2. boradcasting
2. executor memory issue
    1. big partition(some partition get lower no of record and some partiton get big record this problem is skewd so resolve this problem reduce no of partitions)

## executor and driver memory calculation


## spark submit args
./bin/spark-submit \
    --master <master url> \
    --deploy-mode <hadoop-yarn,kubernetes,apache-mesos,standalone> \
    --driver-memory <memory for driver to perfrom count,take,collect operation>GB \
    --executor-memory <memory for task run in executor and also provide cache memory> \
    --executor-cores <no of concurrent execution of task in executor> \
    --jars <comma sepreate> \
    --class <main class> \
    <application jar> \
    [application argument]

## what file system does spark supported
1.HDFS
2.s3
3.local file system

## what is some limitation of spark
1. spark 'in memory error' can be bottleneck,
2. spark uitlize more space compared to hadoop
3. we should have right balance of data accross node in a cluster so that they can process efficiently
   
## list some use case where spark much better then hadoop
1. real time processing >spark is prefered over hadoop for real time quering of the data
2. spark streaming
3. big data processing->spark runs upto 100 times faster than hadoop for processing

## how can spark be used alonside hadoop
hadoop component can be used alonside with spark
1.hdfs
2.mapreduce
3.yarn
4.batch and real time processing

## what is executor memory in spark
the executor memory is basically a measure of how much memory of the worker node need to process the applications
 we can set the property using --executor-memory flag

 ## what is partitions in spark
 Partition is smaller and logical division of large distributed dataset

## what is rdd
rdd is an immutable and folt-tolrant in nature and it distrubuted the data and process in prallel,
any parition of rdd is lost then it have capablity to regenrate it.

## what is role of spark core
1. memory management and fault recovery
2. monitring and scheduling the job in cluster
3. 

## what types of rdd is
1. vector rdd 
2. pair rdd (key value pair)

## what is worker node and master node
worker node(slave) can run the application code in cluster
master node assign the work for worker node and worker node actually perform the assign task 

## what is sparse vector
sparse vector has two parallel arrays one of indices and another is value

## what is dstream
fundamental stream unit is dstream which is basically a series of rdd to process real time data

## what is the significance of sliding window operation.
sliding window basically transform the data with a certain time interval like t2-t1 this is what sliding window is.

## how can you minimize data transfer when working with spark
1. using broadcasting
2. using accumulators

## how can you trigger automatic cleanup in spark handle accumulated metadata
using spark.cleaner.ttl

## what do you understand with schemardd
an schemardd is an rdd that consist of row objects with schema information


## https://sparkbyexamples.com/spark/spark-performance-tuning/

## https://databricks.com/glossary/catalyst-optimizer
<!-- advance level -->
## 1.performance and measure databricks(spark) long running jobs
## 2.bottleneck problem in databricks job  and how to resolve it
## 3.how to select cluster according spark workload in databricks
## 4.memory allocation in databricks spark job
## 5.how databricks job maintain checkpointing
## 6. how to calculate driver memory
## 7.if i read data and do apply some tranformation then describe internal process