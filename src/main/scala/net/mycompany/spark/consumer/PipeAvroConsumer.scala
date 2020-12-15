package net.mycompany.spark.consumer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.avro.{from_avro, to_avro}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, current_date}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types

import java.nio.file.{Files, Paths}

// FIXME add the comments! But more like ScalaDoc not the every line comment!
/**
You are Big Data Developer working for mycompany.net. Your line manager assigned you below task: 

Design and develop pipeline for Hadoop with all steps/layers desired for:
- high performance of end user queries, <- partitioned data by chosen columns!
- easy troubleshooting of any issues within pipeline, <- monitor table?
- low consumption of resources both by pipeline processing and end user queries, <- I would prefer to check Spark UI to verify on the cluster how to optimize it!
- high performance of the cluster, <- the same
- procedures for data corrections in case of bugs in the code that may happen corrupting data. <- TODO in the code!
And optionally
To design and develop CI/CD which will build the solution and deploy it (may include manual steps for DevOps) - including scheduling. 

You have below tools available in mycompany.net:
Spark, Hive, Oozie, Cron, Scala, Python, Java, Bash, Kafka, Kafka Connect, Jenkins, Ansible, Maven. 

Data source information:
On the source there are Microservices. One of Entity of this REST API is Customer Entity.
On each change on Customer (INSERT/UPDATE/DELETE) there is an Event generated and AVRO message is sent to Kafka topic Customer.
// https://aseigneurin.github.io/2018/08/02/kafka-tutorial-4-avro-and-schema-registry.html https://github.com/aseigneurin/kafka-tutorial-simple-client
// bin/schema-registry-start etc/schema-registry/schema-registry.properties
// src/main/resources/entity.avsc
// python src/main/resources/register_schema.py http://localhost:8081 entity-avro src/main/resources/entity.avsc
// https://sparkbyexamples.com/spark/spark-streaming-consume-and-produce-kafka-messages-in-avro-format/
Schema is:
{
 "type": "record",
 "namespace": "net.mycompany",
 "name": "Customer",
 "fields": [
 { "name": "eventDateTime",
 "type": "timestamp" },
 { "name": "eventType",
 "type": "enum",
 "symbols" : ["I", "U", "D"] },
 { "name": "data",
 "type": "record",
 "fields": [
 { "name": "CustomerId",
 "type": "long" },
 { "name": "CustomerName",
 "type": "string" },
 { "name": "CustomerAddress",
 "type": "string" }
 ]
 }
 ]
} 

Additional information:
In case of INSERT or UPDATE data field contains data after changes. In case of DELETE data field contains data being deleted. 

At the end there are Business Intelligence Tools which are connecting to Hive tables CUSTOMER and CUSTOMER_HISTORY.
First table contains most recent Customer data. Second table shows how Customer data changed over time.
There is around 1 million rows of current customers and average of 10 changes a day. 

Candidate is asked to:
Prepare to present the design for the pipeline explaining how to address requirements. <- pptx?
Presentation should be supported with code for each used programming/scripting language
  (in case of many tasks planned to be written in the same language 1 per language is fine for us to review).
  Code can be submitted to GitHub if possible.
Provide DDL for Customer and CustomerHistory with suggestion which additional attributes should be there apart of business fields.
 */
object PipeAvroConsumer { // There is around 1 million rows of current customers and average of 10 changes a day.
 val conf: SparkConf = new SparkConf()
   .setAppName("Customer")
   .setMaster("local")

 val spark: SparkContext = new SparkContext(conf)

 val customer = "Customer"
 // high level with sub-object to just work with a DF for a start!

 val dfStream = spark.readStream
   .format("kafka")
   .option("kafka.bootstrap.servers", "192.168.1.100:9092") // TODO properties to make it more configurable
   .option("subscribe", "avro_topic")
   .option("startingOffsets", "earliest") // From starting
   .load
//   .select(
//    from_avro($"namespace", SchemaBuilder.builder().stringType()).as("namespace"),
//    from_avro($"name", SchemaBuilder.builder().stringType()).as("name"))
// alternative way to select avro topic
/**
  When reading the avro_topic of a Kafka topic, decode the binary (Avro) data into structured data.
  The schema of the resulting DataFrame
 */

 val jsonFormatSchema = new String(
  Files.readAllBytes(Paths.get(s"./src/main/resources/$customer.avsc")))


 val customerDF = dfStream.select(from_avro(col("value"), jsonFormatSchema).as(customer)).select(s"$customer.*")
// Hive tables CUSTOMER(contains most recent Customer data) and CUSTOMER_HISTORY(shows how Customer data changed over time) -> save as table!
 customerDF.write
   .partitionBy(PARTITION_DATE) // TODO decide witch partition column would be the best?
   .mode(SaveMode.Append)
   .saveAsTable(customer) // TODO use some global accessible variables to use constance not strings!

// customerDF
//   .select(
//    to_avro($"key").as("key"),
//    to_avro($"value").as("value"))
//   .writeStream
//   .format("kafka")
//   .option("kafka.bootstrap.servers", "192.168.1.100:9092") // TODO properties to make it more configurable
//   .option("article", "avro_topic")
//   .save()

 val CustomerDDL = spark.sql("""
  CREATE EXTERNAL TABLE IF NOT EXISTS `Customer` (`CustomerId` long, `CustomerName` string, `CustomerAddress` string)
  USING parquet
//  LOCATION '/main/resources/'
  OPTIONS (
    `serialization.format` '1',
    path 'hdfs:///user/zeppelin/Customer')""")

 // TODO eventDate/daily should be added to CustomerHistoryDDL for partition purposes
 val CustomerHistoryDDL = spark.sql("""
  CREATE EXTERNAL TABLE IF NOT EXISTS `CustomerHistory` (`eventDateTime` timestamp, `eventType` enum ["I", "U", "D"], `data` record ,`CustomerId` long, `CustomerName` string, `CustomerAddress` string)
  USING parquet
//  LOCATION '/main/resources/'
  OPTIONS (
    `serialization.format` '1',
    path 'hdfs:///user/zeppelin/CustomerHistory')""")

// INSERT / UPDATE part

// Data deduplication when writing into customer table
  customerDF
   .as("logs")
   .merge(
    newDedupedLogs.as("newDedupedLogs"),
    "logs.uniqueId = newDedupedLogs.uniqueId")
   .whenNotMatched()
   .insertAll()
   .execute()

//  Schema validation & Automatic schema evolution

  customerDF.alias("t")
   .merge(
    customerDF.alias("s"),
    "t.CustomerId = s.CustomerId")
   .whenMatched().updateAll()
   .whenNotMatched().insertAll()
   .execute()

//  Performance tuning while merging/updating
//  Reduce the search space for matches by
  //  events.eventDateTime = current_date()

  import org.apache.spark.sql._
  import io.delta.tables._

  // Reset the output aggregates table
  Seq.empty[(Long, Long)].toDF("key", "count").write
    .format("delta").mode("overwrite").saveAsTable("aggregates")

  val deltaTable = DeltaTable.forName("aggregates")

  // Function to upsert `microBatchOutputDF` into Delta table using MERGE
  def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
    // ===================================================
    // For DBR 6.0 and above, you can use Merge Scala APIs
    // ===================================================
    deltaTable.as("t")
      .merge(
        microBatchOutputDF.as("s"),
        "s.key = t.key")
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()


    //DELETE part



} // Provide DDL for Customer and CustomerHistory with suggestion which additional attributes should be there apart of business fields.




/**
 * First table contains most recent Customer data.
 * Second table shows how Customer data changed over time.
 * There is around 1 million rows of current customers and average of 10 changes a day. <- daily partitioned, to has 10 writes and more optiomal data
 *  ... spark writes 256 files per table directory, so it should be optimize on that too!
 *
 * Customer:
 *
 * { "name": "CustomerId",
 * "type": "long" },
 * { "name": "CustomerName",
 * "type": "string" },
 * { "name": "CustomerAddress",
 * "type": "string" }
 * "name": "eventDateTime",
 * "type": "timestamp"
 *
 * CustomerHistory: // In case of INSERT or UPDATE data field contains data after changes. In case of DELETE data field contains data being deleted.
 * state
 * timestamp
 * theChange ??? how to define it?
 */
