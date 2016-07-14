# spark-ddb-streams-asl_2.10

This example helps you to find HOT keys in Dynamodb table using streams.

Disclaimer : I did my best effort by modifying the existing code which helped me to deliver the expected result. Please free to modify/change based on your use-case.


![StreamsToSpark](StreamsToSpark.png)

# Usage

Jumping to usage:
```
spark-submit --executor-memory 3G --packages com.amazonaws:dynamodb-streams-kinesis-adapter:1.0.2 --jars /home/hadoop/amazon-kinesis-client-1.6.3.jar --class org.apache.spark.examples.streaming.TopTenHotKeys /home/hadoop/spark-ddb-streams-asl_2.10-1.6.1.jar CHECKPOINT-TABLE DDB-TABLE https://streams.dynamodb.REGION.amazonaws.com
```
Make sure you download this [KCL jar](https://s3-us-west-2.amazonaws.com/chappidm-dev/public/ddb-streams/amazon-kinesis-client-1.6.3.jar) since it identifies the SHARD_END in checkpointing table. With default KCL, it fails once the shard is closed.

For simplicity here are the jars:

KCL : [jar](https://s3-us-west-2.amazonaws.com/chappidm-dev/public/ddb-streams/amazon-kinesis-client-1.6.3.jar)

streams-kinesis-adapter : [jar](https://s3-us-west-2.amazonaws.com/chappidm-dev/public/ddb-streams/dynamodb-streams-kinesis-adapter-1.0.2.jar)

spark-ddb-streams-asl_2.10-1.6.1.jar: [jar](https://s3-us-west-2.amazonaws.com/chappidm-dev/public/ddb-streams/spark-ddb-streams-asl_2.10-1.6.1.jar)

#Sample Output

```
+--------------------+---+
|             newName|cnt|
+--------------------+---+
|    "abstract_abbey"|151|
|  "academic_ability"|151|
|      "able_ability"|151|
| "abundant_academic"|150|
| "academic_absolute"|150|
|    "abnormal_abbey"|150|
|   "abstract_absent"|150|
|"abundant_abonormal"|150|
|   "abnormal_absent"|150|
|  "absolute_ability"|150|
+--------------------+---+

===Apprx writes in 1min===
+----+
| _c0|
+----+
|2103|
+----+
```

Here, 2103 writes were made (2103/60 = 35IOPs) with above top 10 Keys.

This helps us to figure the HOT keys without adding any logging mechanism on the application end. Also, you can run TRIM_HORIZON, so that you can trace your past 24hr work to find the HOT key at any particular time. Keep in mind, to query the exact timeline you are looking for based on the 'ApproximateCreationDateTime'.

Similarly for partition+Sort key
```
sqlContext.sql("select newName, newInitm count(*) as cnt from interact group by newName, newInitm order by cnt desc limit 10").show()
```

That's it for now. 

Future work: Should maintain a state-table to figure out the hot partition.


