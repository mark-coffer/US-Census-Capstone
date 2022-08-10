
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

object readCsvToParque{

  def main(args:Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("ajaysingala.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    import spark.implicits._

    //2020 YEAR READ ALL FILES AND PUT IT IN ONE RDD
  //   val censusDF_1_2020 = spark.read
  //     .options(Map("delimiter" -> "|"))
  //     .csv("./src/main/scala/output/csv/csv_2020/*12020.csv")
  //  censusDF_1_2020.show(false)

    // val censusDF_2_2020 = spark.read
    //   .options(Map("delimiter" -> "|"))
    //   .csv("./src/main/scala/output/csv/csv_2020/*22020.csv")
    // //censusDF_2_2020.show(false)

    // val censusDF_3_2020 = spark.read
    //   .options(Map("delimiter" -> "|"))
    //   .csv("./src/main/scala/output/csv/csv_2020/*32020.csv")
    // //censusDF_3_2020.show(false)

   //censusDF_1_2020.write.parquet("./src/main/scala/output/parquet/2020/parquet_1_2020")
   //censusDF_2_2020.write.parquet("./src/main/scala/output/parquet/2020/parquet_2_2020")
    //censusDF_3_2020.write.parquet("./src/main/scala/output/parquet/2020/parquet_3_2020")


  //2010 YEAR
  //   val censusDF_1_2010 = spark.read
  //     .options(Map("delimiter" -> ","))
  //     .csv("./src/main/scala/output/csv/csv_2010/*12010.csv")
  //  censusDF_1_2010.show(false)

    // val censusDF_2_2010 = spark.read
    //   .options(Map("delimiter" -> ","))
    //   .csv("./src/main/scala/output/csv/csv_2010/*22010.csv")
    //censusDF_2_2020.show(false)

   //censusDF_1_2010.write.mode("overwrite").parquet("./src/main/scala/output/parquet/2010/parquet_1_2010")
   //censusDF_2_2010.write.parquet("./src/main/scala/output/parquet/2010/parquet_2_2010")
    

    //2000 YEAR
    // val censusDF_1_2000 = spark.read
    //   .options(Map("delimiter" -> ","))
    //   .csv("./src/main/scala/output/csv/csv_2000/*001.csv")
   //censusDF_1_2000.show(false)

    // val censusDF_2_2000 = spark.read
    //   .options(Map("delimiter" -> ","))
    //   .csv("./src/main/scala/output/csv/csv_2000/*002.csv")
    // //censusDF_2_2020.show(false)

   //censusDF_1_2000.write.parquet("./src/main/scala/output/parquet/2000/parquet_1_2000")
   //censusDF_2_2000.write.parquet("./src/main/scala/output/parquet/2000/parquet_2_2000")
    
    
    // // Read Parquet file into DataFrame.
    // println("Read the Parquet file...")
    val parqDF = spark.read.parquet("./src/main/scala/output/parquet/2020/parquet_1_2020")
    parqDF.printSchema()
    parqDF.show()

  }
}

