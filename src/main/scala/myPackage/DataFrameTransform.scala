package myPackage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

case class DataFrameTransform() {
  def transformCountry(df: DataFrame): DataFrame = {
    val df_country_count = df
      .where("COUNTRY is not null")
      .groupBy("COUNTRY")
      .count()
      .sort(desc("count"))
      .limit(10)
    return df_country_count
  }

  def transformAirplane(df: DataFrame): DataFrame = {
    val df_aircraft_count = df
      .withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
      .groupBy("TYPE_AIRCRAFT")
      .count()
      .sort(desc("count"))
      .limit(3)
    return df_aircraft_count
  }

  def transformYearAirplane(df: DataFrame): DataFrame = {
    val df_year_aircraft_count = df
      .withColumnRenamed("TYPE AIRCRAFT", "TYPE_AIRCRAFT")
      .withColumnRenamed("YEAR MFR", "YEAR_MFR")
      .where("YEAR_MFR is not null")
      .groupBy("TYPE_AIRCRAFT", "YEAR_MFR")
      .count()
      .sort(desc("count"))
      .limit(5)
    return df_year_aircraft_count
  }
}
