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

