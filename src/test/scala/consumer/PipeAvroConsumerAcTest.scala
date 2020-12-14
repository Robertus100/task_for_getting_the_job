package consumer

import net.mycompany.spark.consumer.PipeAvroConsumer
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSpec

import scala.util.parsing._

class PipeAvroConsumerAcTest extends FunSpec {
  implicit lazy val spark: SparkSession = SparkSession.builder.master("local[*]")
    .appName("PipeAvroConsumerAcTest").getOrCreate // FIXME spark could be get out to the separated object we will extentd in the tests!
 
  val schemaSample: String = """{
                               | "type": "record",
                               | "namespace": "net.mycompany",
                               | "name": "Customer",
                               | "fields": [
                               | { "name": "eventDateTime",
                               | "type": "timestamp" },
                               | { "name": "eventType",
                               | "type": "enum",
                               | "symbols" : ["I", "U", "D"] },
                               | { "name": "data",
                               | "type": "record",
                               | "fields": [
                               | { "name": "CustomerId",
                               | "type": "long" },
                               | { "name": "CustomerName",
                               | "type": "string" },
                               | { "name": "CustomerAddress",
                               | "type": "string" }
                               | ]
                               | }
                               | ]
                               |} """.stripMargin

//  In case of INSERT or UPDATE data field contains data after changes. In case of DELETE data field contains data being deleted.
  it ("should verify the INSERT") {
    val result = PipeAvroConsumer.load
    val expected = ""
    assert(expected === result)
  }

  // Parse a set of JSON files into an RDD of Map elements
  import scala.util.parsing.json.JSON
  val result = schemaSample
  val map_result=result.map(pair => JSON.parseFull(pair._2).get.asInstanceOf[Map[String,String]])
  for (record <- map_result.take(2))
    println(record.getOrElse("firstName",null))

}
/**
package com.pmi.ocean.idd.staging_layer.river_hdfs_migrator

import java.io.{File, InputStream}
import java.util.UUID

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.databricks.spark.avro._
import com.pmi.ocean.idd.commons.enterprise_landing.LoadType.InitialLoad
import com.pmi.ocean.idd.commons.enterprise_landing._
import com.pmi.ocean.idd.landing_layer.river_hdfs_migrator.Main.{parseDataType, parseDelimiterOption}
import com.pmi.ocean.idd.landing_layer.river_hdfs_migrator.{DiffEnterpriseLandingStatus, GenericHdfsMigrationExtractor, GenericHdfsMigratorPipeline}
import com.pmi.ocean.spark.common.SparkSuite
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class MainACTest extends SparkSuite {

  test("Generic river HDFS/FS Migration Main with prepareStatus") {
    implicit lazy val fileSystem: FileSystem = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
    val elTableName = "data"
    val sourceSystemCode = "FlexL1"
    val inputPath: String = s"/tmp/${UUID.randomUUID.toString}/input/${UUID.randomUUID.toString}.csv"
    if (fileSystem.exists(new Path(inputPath))) fileSystem.delete(new Path(inputPath), true)

    Array(Array("a", "b", "c"), Array("a", "b", "c", "d")).foreach {
      case Array(a, b, c) => import spark.implicits._
        prepareData((0 to 1000).map { x => ("Flex_ID",
          "GCRS_ID",
          "DCE_ID",
          "DeviceCodentify",
          "Received_DT",
          "Received_Location",
          "Return_Remark",
          "L1_ErrorCode",
          "L1_Verbatim",
          "Inspector",
          "L1_InspectionStart_DT",
          "Inspection_DT",
          "CycleCount",
          "CleaningCycle",
          "IQOS_Connect",
          "L2_Required",
          "L2_Shipment_DT",
          "Modified_DT",
          "MarketCountryISO2")
        }.toDF("Flex_ID",
          "GCRS_ID",
          "DCE_ID",
          "DeviceCodentify",
          "Received_DT",
          "Received_Location",
          "Return_Remark",
          "L1_ErrorCode",
          "L1_Verbatim",
          "Inspector",
          "L1_InspectionStart_DT",
          "Inspection_DT",
          "CycleCount",
          "CleaningCycle",
          "IQOS_Connect",
          "L2_Required",
          "L2_Shipment_DT",
          "Modified_DT",
          "MarketCountryISO2"), inputPath)
      case Array(a, b, c, d) => import spark.implicits._
        prepareData((0 to 1000).map { x => ("Flex_ID",
          "GCRS_ID",
          "DCE_ID",
          "DeviceCodentify",
          "Received_DT",
          "Received_Location",
          "Return_Remark",
          "L1_ErrorCode",
          "L1_Verbatim",
          "Inspector",
          "L1_InspectionStart_DT",
          "Inspection_DT",
          "CycleCount",
          "CleaningCycle",
          "IQOS_Connect",
          "L2_Required",
          "L2_Shipment_DT",
          "Modified_DT",
          "MarketCountryISO2")
        }.toDF("Flex_ID",
          "GCRS_ID",
          "DCE_ID",
          "DeviceCodentify",
          "Received_DT",
          "Received_Location",
          "Return_Remark",
          "L1_ErrorCode",
          "L1_Verbatim",
          "Inspector",
          "L1_InspectionStart_DT",
          "Inspection_DT",
          "CycleCount",
          "CleaningCycle",
          "IQOS_Connect",
          "L2_Required",
          "L2_Shipment_DT",
          "Modified_DT",
          "MarketCountryISO2")
        .withColumn("Return_Channel", lit("Return_Channel"))
        .withColumn("Product_Type", lit("Product_Type"))
        .withColumn("DeviceVersion", lit("DeviceVersion"))
        .withColumn("Manufacturing_Center", lit("Manufacturing_Center"))
        .withColumn("Production_DT", lit("Production_DT"))
        .withColumn("GCRS_Creation_DT", lit("GCRS_Creation_DT"))
        .withColumn("DCE_Creation_DT", lit("DCE_Creation_DT")), inputPath)
    }
    val expectedCount = spark.read.option("header", "true").csv(inputPath).count
    val outputPath: String = s"/tmp/${UUID.randomUUID.toString}/output/"
    object Conf extends PipelineConfiguration(
      spark.sparkContext.getConf.getOption(s"spark.$elTableName.default_initial_source_date")) {
      override def environment: String = "dev"
      override val localCachePath: String = outputPath
    }
    val source = SourceMetadata(sourceSystemCode, elTableName, Conf.environment)
    object TestDiffEnterpriseLandingStatus extends DiffEnterpriseLandingStatus {
      override def getLandingFilenamesRDD(bucket: String, objectKeyPrefix: String): RDD[String] = spark.emptyDataFrame.rdd.map{x => s"$x"}
    }
    implicit val diffEnterpriseLandingStatus: DiffEnterpriseLandingStatus = TestDiffEnterpriseLandingStatus
    object Status extends EnterpriseLandingStatus(source, Conf, spark.emptyDataFrame.rdd.map{x => s"$x"}) {
      override val lastLoadedDate: Option[String] = Some("2020-01-02")
      override def nextLoad: EnterpriseLandingNextLoadDescription =
        EnterpriseLandingNextLoadDescription(
          source,
          Some("2020-01-03"),
          "2020-01-04",
          "utc",
          InitialLoad,
          nowUtc
        )
    }
    implicit val status: EnterpriseLandingStatus = Status
    implicit val sourceMetadataFromLoader: Option[EnterpriseLandingNextLoadDescription] = Some(status.nextLoad)
    object TestEnterpriseLandingLoaderNoCache extends EnterpriseLandingLoaderNoCache {
      var count = 0L
      override def uploadToS3(
                               sourcePath: File, s3Bucket: String, s3ObjectKeyPrefix: String, fileExtension: String
                             )(implicit spark: SparkSession): Boolean = {
        fileExtension match {
          case ".avro" =>
            count = spark.read.avro(s"${sourcePath.getAbsolutePath}/*.avro").count
            true
          case _ => false
        }
      }
      override def uploadS3ObjectByStream(s3Client: AmazonS3, input: InputStream, name: String, metadata: ObjectMetadata
                                          , bucket: String, objectKeyPrefix: String): Unit = {}
    }
    GenericHdfsMigratorPipeline(
      new GenericHdfsMigrationExtractor(
        SourceHdfsPath(inputPath,
          parseDataType("CSV"),
          parseDelimiterOption(Some(","))
        ),
        sourceSystemCode
      ),
      TestEnterpriseLandingLoaderNoCache
    ).run

    assert(expectedCount === TestEnterpriseLandingLoaderNoCache.count)

    if (fileSystem.exists(new Path(inputPath))) fileSystem.delete(new Path(inputPath), true)
    if (fileSystem.exists(new Path(outputPath))) fileSystem.delete(new Path(outputPath), true)
  }

  test ("should validate with csv files the load") {
    implicit lazy val fileSystem: FileSystem = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
    val elTableName = "data"
    val sourceSystemCode = "FlexL1"
    val inputPath: String = s"/tmp/${UUID.randomUUID.toString}/input/${UUID.randomUUID.toString}.csv"
    if (fileSystem.exists(new Path(inputPath))) fileSystem.delete(new Path(inputPath), true)

    Array("L1_Result_2.csv", "L1_Result_3.csv").foreach { file =>
      FileUtils.copyFile(
        new File(getClass.getResource(s"/$file").getPath),
        new File(s"$inputPath/$file")
      )
    }
    val expectedCount = spark.read.option("header", "true").csv(inputPath).count
    val outputPath: String = s"/tmp/${UUID.randomUUID.toString}/output/"
    object Conf extends PipelineConfiguration(
      spark.sparkContext.getConf.getOption(s"spark.$elTableName.default_initial_source_date")) {
      override def environment: String = "dev"
      override val localCachePath: String = outputPath
    }
    val source = SourceMetadata(sourceSystemCode, elTableName, Conf.environment)
    object TestDiffEnterpriseLandingStatus extends DiffEnterpriseLandingStatus {
      override def getLandingFilenamesRDD(bucket: String, objectKeyPrefix: String): RDD[String] = spark.emptyDataFrame.rdd.map{x => s"$x"}
    }
    implicit val diffEnterpriseLandingStatus: DiffEnterpriseLandingStatus = TestDiffEnterpriseLandingStatus
    object Status extends EnterpriseLandingStatus(source, Conf, spark.emptyDataFrame.rdd.map{x => s"$x"}) {
      override val lastLoadedDate: Option[String] = Some("2020-01-02")
      override def nextLoad: EnterpriseLandingNextLoadDescription =
        EnterpriseLandingNextLoadDescription(
          source,
          Some("2020-01-03"),
          "2020-01-04",
          "utc",
          InitialLoad,
          nowUtc
        )
    }
    implicit val status: EnterpriseLandingStatus = Status
    implicit val sourceMetadataFromLoader: Option[EnterpriseLandingNextLoadDescription] = Some(status.nextLoad)
    object TestEnterpriseLandingLoaderNoCache extends EnterpriseLandingLoaderNoCache {
      var count = 0L
      var fieldNames = Array.empty[String]
      override def uploadToS3(
                               sourcePath: File, s3Bucket: String, s3ObjectKeyPrefix: String, fileExtension: String
                             )(implicit spark: SparkSession): Boolean = {
        fileExtension match {
          case ".avro" =>
            count = spark.read.avro(s"${sourcePath.getAbsolutePath}/*.avro").count
            fieldNames = spark.read.avro(s"${sourcePath.getAbsolutePath}/*.avro").schema.fieldNames
            true
          case _ => false
        }
      }
      override def uploadS3ObjectByStream(s3Client: AmazonS3, input: InputStream, name: String, metadata: ObjectMetadata
                                          , bucket: String, objectKeyPrefix: String): Unit = {}
    }
    GenericHdfsMigratorPipeline(
      new GenericHdfsMigrationExtractor(
        SourceHdfsPath(inputPath,
          parseDataType("CSV"),
          parseDelimiterOption(Some(","))
        ),
        sourceSystemCode
      ),
      TestEnterpriseLandingLoaderNoCache
    ).run

    TestEnterpriseLandingLoaderNoCache.fieldNames should contain allOf (
      "flex_id",
      "gcrs_id",
      "dce_id",
      "device_codentify",
      "received_date",
      "received_location",
      "return_remark",
      "l1_error_code",
      "l1_verbatim",
      "inspector",
      "l1_inspection_start_date",
      "inspection_date",
      "cycle_count",
      "cleaning_cycle",
      "iqos_connect",
      "l2_required",
      "l2_shipment_date",
      "modified_date",
      "market_country_iso2",
      "return_channel",
      "product_type",
      "device_version",
      "manufacturing_center",
      "production_date",
      "gcrs_creation_date",
      "dce_creation_date"
    )

    assert(expectedCount === TestEnterpriseLandingLoaderNoCache.count)

    if (fileSystem.exists(new Path(inputPath))) fileSystem.delete(new Path(inputPath), true)
    if (fileSystem.exists(new Path(outputPath))) fileSystem.delete(new Path(outputPath), true)
  }

  private def prepareData(df: DataFrame, dstPath: String)(implicit fileSystem: FileSystem): Unit = {
    val inputTmpPath: String = s"/tmp/${UUID.randomUUID.toString}/tmpInput/${UUID.randomUUID.toString}.csv"
    df.write.option("header", "true").csv(inputTmpPath)

    if (!fileSystem.exists(new Path(dstPath))) fileSystem.mkdirs(new Path(dstPath))
    spark.read.csv(inputTmpPath).inputFiles.foreach { filePath =>
      FileUtil.copy(
        fileSystem, new Path(filePath)
        , fileSystem, new Path(dstPath)
        , true, spark.sparkContext.hadoopConfiguration)
    }
    if (fileSystem.exists(new Path(inputTmpPath))) fileSystem.delete(new Path(inputTmpPath), true)
  }
}
*/
