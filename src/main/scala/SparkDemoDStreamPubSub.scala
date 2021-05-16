import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

object SparkDemoDStreamPubSub {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    import org.apache.spark._

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    val projectID = "double-archive-310010" //"quixotic-folio-309721"
    val subscription = "subscription_uni" //"test-sub"

    val lines = PubsubUtils.createStream(
        ssc,
        projectID,
        Some("test"),
        subscription, // Cloud Pub/Sub subscription for incoming tweets
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    //lines.print()

    val sc = SparkSession.builder()
      .master("local[*]")
      .appName("pub/sub streaming")
      .getOrCreate()

    /*val schema = StructType(
      Seq(
        StructField(name = "message", dataType = StringType, nullable = false),
        //StructField(name = "load_timestamp", dataType = TimestampType, nullable = false),
      )
    )*/

    val schema = StructType(
      Seq(
        //StructField(name = "message", dataType = StringType, nullable = false),
        StructField(name = "COUNTRY", dataType = StringType, nullable = false),
        StructField(name = "TYPE_AIRCRAFT", dataType = IntegerType, nullable = false),
        StructField(name = "YEAR_MFR", dataType = IntegerType, nullable = false),
        StructField(name = "LOAD_TIME", dataType = TimestampType, nullable = false),
      )
    )

    lines.foreachRDD(rdd => {
      val current_timestamp = new Timestamp(System.currentTimeMillis())

      //val rows = rdd.map(l => Row(l, current_timestamp))
      def row(line: List[String]): Row = Row(line(0), line(1).toInt, line(2).toInt, current_timestamp)
      val rows = rdd
      .map(_.split(",").to[List]).map(row)
        //.flatMap(line => line.split(","))
        //.map(l => Row(l))

      val df = sc.createDataFrame(rows, schema)
      df.show()

      val df_country_count = df.groupBy("COUNTRY", "LOAD_TIME").count()
      df_country_count.sort(desc("count")).limit(10).show()
      df_country_count
        .limit(10)
        //.coalesce(1)
        .write
        //.csv("/Users/vladislavtihonov/Documents/CSV/tmp/test/country")
      .format("bigquery")
      .option("table", "my_dataset.pub_sub_messages_country")//"database.pub_sub_messages")
      .option("temporaryGcsBucket", "myexamplebucket251")//"sgu_bigquery_tmp")
      //.mode(SaveMode.Overwrite)
      .mode(SaveMode.Append)
      .save()


      val df_aircraft_count = df.groupBy("TYPE_AIRCRAFT", "LOAD_TIME").count()
      df_aircraft_count.sort(desc("count")).limit(3).show()
      df_aircraft_count
        .limit(3)
        //.coalesce(1)
        .write
        //.csv("/Users/vladislavtihonov/Documents/CSV/tmp/test/aircraft")
        .format("bigquery")
        .option("table", "my_dataset.pub_sub_messages_aircraft")//"database.pub_sub_messages")
        .option("temporaryGcsBucket", "myexamplebucket251")//"sgu_bigquery_tmp")
        //.mode(SaveMode.Overwrite)
        .mode(SaveMode.Append)
        .save()


        val df_year_aircraft_count = df.groupBy("TYPE_AIRCRAFT", "YEAR_MFR", "LOAD_TIME").count()
      //val df_year_aircraft_count = df_year_aircraft.
      //df_year_aircraft_count.withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
      //df_year_aircraft_count.withColumnRenamed("YEAR MFR", "YEAR_MFR")
      df_year_aircraft_count.sort(desc("count")).limit(5).show()
      df_year_aircraft_count
        .limit(5)
        //.coalesce(1)
        .write
        //.csv("/Users/vladislavtihonov/Documents/CSV/tmp/test/year")
      .format("bigquery")
      .option("table", "my_dataset.pub_sub_messages_year")//"database.pub_sub_messages")
      .option("temporaryGcsBucket", "myexamplebucket251")//"sgu_bigquery_tmp")
      //.mode(SaveMode.Overwrite)
      .mode(SaveMode.Append)
      .save()
    })
//US,1,1936
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}