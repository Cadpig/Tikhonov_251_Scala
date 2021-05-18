import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark

import myPackage.DataFrameTransform

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.google.cloud.spark.bigquery._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.desc

case class Plane(N_NUMBER: BigInt, SERIAL_NUMBER:String, MFR_MDL_CODE:BigInt,
                 ENG_MFR_MDL:Double, YEAR_MFR:Double, TYPE_REGISTRANt:Double, NAME:String, STREET:String,
                 STREET2:String, CITY:String, STATE:String, ZIP_CODE:BigInt, REGION:String, COUNTY :Double,
                 COUNTRY:String, LAST_ACTION_DATE :BigInt, CERT_ISSUE_DATE :Double, CERTIFICATION:String, TYPE_AIRCRAFT :Int,
                 TYPE_ENGINE :Int, STATUS_CODE:String, MODE_S_CODE :BigInt, FRACT_OWNER :Boolean, AIR_WORTH_DATE :BigInt,
                 OTHER_NAMES1:String, OTHER_NAMES2:String, OTHER_NAMES3:String, OTHER_NAMES4:String,
                 OTHER_NAMES5:String, EXPIRATION_DATE :Double, UNIQUE_ID :BigInt, KIT_MFR:String, KIT_MODEL:String,
                 MODE_S_CODE_HEX :String, X35 :Int)

object SparkDemo {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    val transform = new DataFrameTransform()

    val inputPath = "gs://myexamplebucket251/faa_registration.csv"
    // val bucket = "myexamplebucket251"

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val schema = StructType.fromDDL("N_NUMBER bigint, SERIAL_NUMBER string, MFR_MDL_CODE bigint, " +
      "ENG_MFR_MDL double, YEAR_MFR double, TYPE_REGISTRANT double, NAME string, STREET string, " +
      "STREET2 string, CITY string, STATE string, ZIP_CODE bigint, REGION string, COUNTY double, " +
      "COUNTRY string, LAST_ACTION_DATE bigint, CERT_ISSUE_DATE double, CERTIFICATION string, TYPE_AIRCRAFT int," +
      "TYPE_ENGINE int, STATUS_CODE string, MODE_S_CODE bigint, FRACT_OWNER boolean, AIR_WORTH_DATE bigint, " +
      "OTHER_NAMES1 string, OTHER_NAMES2 string, OTHER_NAMES3 string, OTHER_NAMES4 string, " +
      "OTHER_NAMES5 string, EXPIRATION_DATE double, UNIQUE_ID bigint, KIT_MFR string, KIT_MODEL string, " +
      "MODE_S_CODE_HEX string, X35 int")


    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(inputPath)
      //.load("/Users/vladislavtihonov/Downloads/archive/faa_registration.csv")


    import spark.implicits._

    implicit val enc: Encoder[Plane] = Encoders.product[Plane]

    val ds = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .load(inputPath) // DataFrame
      //.load("/Users/vladislavtihonov/Downloads/archive/faa_registration.csv")
      .as[Plane] // DataSet


    //.load("/Users/vladislavtihonov/Downloads/archive/faa_registration.csv")

    val df_country_count = transform.transformCountry(df)/*df
      .where("COUNTRY is not null")
      .groupBy("COUNTRY")
      .count()
      .sort(desc("count"))
      .limit(10)*/

    df_country_count.cache()

    df_country_count.show()

    val df_aircraft_count = transform.transformAirplane(df)/*df
      .withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
      .groupBy("TYPE_AIRCRAFT")
      .count()
      .sort(desc("count"))
      .limit(3)*/
    //df_aircraft_count.withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")

    df_aircraft_count.cache()

    df_aircraft_count.show()

    val df_year_aircraft_count = transform.transformYearAirplane(df)/*df
      .withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
      .withColumnRenamed("YEAR MFR", "YEAR_MFR")
      .where("YEAR_MFR is not null")
      .groupBy("TYPE_AIRCRAFT", "YEAR_MFR")
      .count()
      .sort(desc("count"))
      .limit(5)*/
    //val df_year_aircraft_count = df_year_aircraft.
    //df_year_aircraft_count.withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
    //df_year_aircraft_count.withColumnRenamed("YEAR MFR", "YEAR_MFR")

    df_year_aircraft_count.cache()

    df_year_aircraft_count.show()

   df_country_count
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.country")
      .save()

    df_aircraft_count
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.aircraft")
      .save()

    df_year_aircraft_count
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.year_aircraft")
      .save()


    val ds_country_count = ds
      //.filter("COUNTRY is not null")
      .filter($"COUNTRY".isNotNull)
      .groupBy($"COUNTRY")
      .count()
      .sort(desc("count"))
      .limit(10)

    ds_country_count.cache()

    ds_country_count.show()

    val ds_aircraft_count = ds
      .groupBy($"TYPE_AIRCRAFT")
      .count()
      .sort(desc("count"))
      .limit(3)

    ds_aircraft_count.cache()

    ds_aircraft_count.show()


    val ds_year_aircraft_count = ds
      .filter($"YEAR_MFR".isNotNull)
      .groupBy($"TYPE_AIRCRAFT", $"YEAR_MFR")
      .count()
      .sort(desc("count"))
      .limit(5)

    ds_year_aircraft_count.cache()

    ds_year_aircraft_count.show()


    ds_country_count
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.ds_country")
      .save()

    ds_aircraft_count
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.ds_aircraft")
      .save()

    ds_year_aircraft_count
      .write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", "myexamplebucket251")
      .option("table", "my_dataset.ds_year_aircraft")
      .save()


  }
}