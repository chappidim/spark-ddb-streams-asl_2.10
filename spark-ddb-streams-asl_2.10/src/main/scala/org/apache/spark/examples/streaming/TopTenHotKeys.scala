package org.apache.spark.examples.streaming

import java.nio.ByteBuffer

import scala.util.Random

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._
import org.json4s.DefaultFormats

/**
 * Consumes messages from a DDB streams and identifies hotKeys.
 *
 *
 * Usage: TopTenHotKeys <app-name> <stream-name> <endpoint-url> <region-name>
 *   <app-name> is the name of the consumer app, used to track the read data in DynamoDB
 *   <DDBStreamsTable-name> name of the DDB streams table (ie. mySparkStream)
 *   <endpoint-url> endpoint of the Kinesis service
 *     (e.g. https://kinesis.us-east-1.amazonaws.com)
 *
 */
object TopTenHotKeys extends Logging {
  def main(args: Array[String]) {
    // Check that all required args were passed in.
    if (args.length != 3) {
      System.err.println(
        """
          |Usage: TopTenHotKeys <app-name> <stream-name> <endpoint-url> <region-name>
          |
          |    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
          |    <DDBStreamsTable-name> is the name of the Kinesis stream
          |    <endpoint-url> is the endpoint of the DDBStreams service
          |                   (e.g. https://streams.dynamodb.us-west-2.amazonaws.com)
        """.stripMargin)
      System.exit(1)
    }


    // Populate the appropriate variables from the given args
    val Array(appName, tableName, endpointUrl) = args


    // Determine the number of shards from the stream using the low-level Kinesis Client
    // from the AWS Java SDK.
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    val ddbClient = new AmazonDynamoDBClient(credentials)
    //ddbClient.setEndpoint(endpointUrl)
    ddbClient.setRegion(Region.getRegion(Regions.US_WEST_2));
    val streamArn = ddbClient.describeTable(new DescribeTableRequest().withTableName(tableName)).getTable().getLatestStreamArn()
    val dbSClient = new AmazonDynamoDBStreamsClient(credentials)
    dbSClient.setRegion(Region.getRegion(Regions.US_WEST_2))
    val numShards = dbSClient.describeStream(new DescribeStreamRequest().withStreamArn(streamArn)).getStreamDescription().getShards().size


    // In this example, we're going to create 1 Receiver/input DStream for each shard.
    // This is not a necessity; if there are less receivers/DStreams than the number of shards,
    // then the shards will be automatically distributed among the receivers and each receiver
    // will receive data from multiple shards.
    val numStreams = numShards

    // Spark Streaming batch interval of 60sec
    val batchInterval = Milliseconds(60000)

    // checkpoint interval is the interval at which the DynamoDB is updated with information
    // on sequence number of records that have been received. Same as batchInterval for this
    // example.
    val checkpointInterval = batchInterval

    // Get the region name from the endpoint URL to save KCL metadata in
    // DynamoDB checkpoint  of the same region as the DDB stream
    val regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName()

    // Setup the SparkConfig and StreamingContext
    val sparkConfig = new SparkConf().setAppName("TopTenHotKeys")
    val ssc = new StreamingContext(sparkConfig, batchInterval)

    // Create the DDB DStreams
    val ddbStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, appName, streamArn, endpointUrl, regionName,
        InitialPositionInStream.LATEST, checkpointInterval, StorageLevel.MEMORY_AND_DISK_2)
    }

    // Union all the streams
    val unionStreams = ssc.union(ddbStreams)

    // Convert each line of Array[Byte] to String, and split into words
    val words = unionStreams.flatMap(byteArray => new String(byteArray).split(" "))
    words.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      //val interact = rdd.map(line => {parse(line)}).map(line => {val newName = compact(line \ "Dynamodb" \ "NewImage" \ "name" \ "S"); val newId = compact(line \ "Dynamodb" \ "NewImage" \ "id" \ "N"); (Record(newName,newId))}).toDF()
      val interact = rdd.map(line => {parse(line)}).map(line => {val newName = compact(line \ "Dynamodb" \ "NewImage" \ "rname" \ "S"); val newTime = compact(line \ "Dynamodb" \ "NewImage" \ "rtime" \ "S"); (Record(newName,newTime))}).toDF()
      // Register as table
      interact.registerTempTable("interact")

      // Do word count on table using SQL and print it
      sqlContext.sql("select newName, count(*) as cnt from interact group by newName order by cnt desc limit 10").show()
      println(s"===Apprx writes on table in 1min===")
      sqlContext.sql("select count(*) from interact").show()
      println(s"========= $time =========")
    })

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}

case class Record(newName: String, newTime: String)

object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
