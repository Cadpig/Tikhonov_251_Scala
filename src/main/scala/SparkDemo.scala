import org.apache.log4j.{Level, Logger}
import org.apache.spark

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.google.cloud.spark.bigquery._
import org.apache.spark.sql.functions.desc

object SparkDemo {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    val inputPath = "gs://myexamplebucket251/faa_registration.csv"
    // val bucket = "myexamplebucket251"

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(inputPath)
    //.load("/Users/vladislavtihonov/Downloads/archive/faa_registration.csv")

    val df_country_count = df.groupBy("COUNTRY").count()
    df_country_count.sort(desc("count")).limit(10).show()

    val df_aircraft_count = df.groupBy("TYPE AIRCRAFT").count()
    //df_aircraft_count.withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
    df_aircraft_count.sort(desc("count")).limit(3).show()

    val df_year_aircraft_count = df.groupBy("TYPE AIRCRAFT", "YEAR MFR").count()
    //val df_year_aircraft_count = df_year_aircraft.
    //df_year_aircraft_count.withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
    //df_year_aircraft_count.withColumnRenamed("YEAR MFR", "YEAR_MFR")
    df_year_aircraft_count.sort(desc("count")).limit(5).show()

    df_country_count.sort(desc("count")).limit(3)
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.country")
      .save()

    df_aircraft_count.sort(desc("count"))
      .withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
      .limit(3)
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.aircraft")
      .save()

    df_year_aircraft_count.sort(desc("count"))
      .withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
      .withColumnRenamed("YEAR MFR", "YEAR_MFR")
      .limit(5)
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.year_aircraft")
      .save()

  }
}