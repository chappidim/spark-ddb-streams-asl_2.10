# spark-ddb-streams-asl_2.10

This example helps you to find HOT keys in Dynamodb table using streams.

Disclaimer : I did my best effort by modifying the existing code which helped me to deliver the expected result. Please free to modify/change based on your use-case.


![StreamToSpark](StreamToSpark.png)

# Usage

Jumping to usage:
spark-submit --executor-memory 3G --packages com.amazonaws:dynamodb-streams-kinesis-adapter:1.0.2 --jars /home/hadoop/amazon-kinesis-client-1.6.3.jar --class org.apache.spark.examples.streaming.TopTenHotKeys /home/hadoop/spark-ddb-streams-asl_2.10-1.6.1.jar CHECKPOINT-TABLE DDB-TABLE https://streams.dynamodb.REGION.amazonaws.com

Make sure you download this [KCL jar](https://s3-us-west-2.amazonaws.com/chappidm-dev/public/ddb-streams/amazon-kinesis-client-1.6.3.jar) since it identifies the SHARD_END in checkpointing table. With default KCL, it fails once the shard is closed.

