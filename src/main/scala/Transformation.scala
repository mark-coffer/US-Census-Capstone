

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Transformation {

  val spark:SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext
  val bucketName = "revature-ajay-big-data-1452"
  val accessKey = "AKIA4OK5FKIYV3DQPT7K"
  val secretKey = "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"

  val statesList =List("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY")

  val regionsList = Map(("AL", "South"), ("AK", "West"), ("AZ", "West"), ("AR", "South"), ("CA", "South"), ("CO", "West"), ("CT", "Northeast"),
    ("DE", "South"), ("DC", "South"), ("FL", "South"), ("GA", "South"), ("HI", "West"), ("ID", "West"), ("IL", "Midwest"), ("IN", "Midwest"),
    ("IA", "Midwest"), ("KS", "Midwest"), ("KY", "South"), ("LA", "South"), ("ME", "Northeast"), ("MD", "South"), ("MA", "Northeast"), ("MI", "Midwest"),
    ("MN", "Midwest"), ("MS", "South"), ("MO", "Midwest"), ("MT", "West"), ("NE", "Midwest"), ("NV", "West"), ("NH", "Northeast"), ("NJ", "Northeast"),
    ("NM", "West"), ("NY", "Northeast"), ("NC", "South"), ("ND", "Midwest"), ("OH", "Midwest"), ("OK", "South"), ("OR", "West"), ("PA", "Northeast"),
    ("PR", "Not In a Region"), ("RI", "Northeast"), ("SC", "South"), ("SD", "Midwest"), ("TN", "South"), ("TX", "South"), ("UT", "West"),
    ("VT", "Northeast"), ("VA", "South"), ("WA", "West"), ("WV", "South"), ("WI", "Midwest"), ("WY", "West"))

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
  spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  import spark.implicits._

  def main(args:Array[String]): Unit = {


    downloadParquet()
  }

  def downloadParquet(): Unit = {
    //download 6 files from S3, excluding geo files
    val rawData1 = spark.read.option("delimiter", ",").parquet("s3a://revature-ajay-big-data-1452/census/2000/census_2000_1/people.parquet")
//    val rawData2 = spark.read.option("delimiter", "|").parquet("")
//    val rawData3 = spark.read.option("delimiter", "|").parquet("")

    rawData1.printSchema()

    // another way to read DF with hadoopConfiguration.
    //      val df2 = spark.sparkContext.textFile(s"s3a://${bucketName}/testFileForS3.csv")
    //      println(df2.name)
    //      println("##Get data Using collect")
    //      df2.collect().foreach(f=>{
    //        println(f)
    //      })

    //val newOptimizedDF = modifyDFcolumnsForFile01(rawData1)
    //uploadOptimizedData(newOptimizedDF)
  }

  def joinMultipleDFs(df: DataFrame*): Unit = {

//    val df = df
//    val df2 = df

    //make this to loop through and add dataframe to array of dataframes?
    //val joinedDF = modifyDFcolumns(df).union(modifyDFcolumns(df2)).toDF()

    //joinedDF.coalesce(1).createOrReplaceTempView("joinedDF")
    //val totalPopDF01 = spark.sql("select _c1, max(_c5), max(_c6), max(_c7), max(_c8), max(_c9), max(_c10), max(_c11), max(_c12) joinedDF where _c1 == 'OH' or _c1 == 'AL' group by _c1")
    //uploadOptimizedData(joinedDF)
  }

  def modifyDFcolumnsForFile01(df: DataFrame): DataFrame = {
    val cast_df = df.select(df.columns.map {
      case column@"_c0"=>col(column).cast("String").as(column)
      case column@"_c1"=>col(column).cast("String").as(column)
      case column => col(column).cast("Integer").as(column)
    }: _*)

    val cleaned_DF = cast_df.drop("_c0", "_c2", "_c3", "_c4")
    cleaned_DF.createOrReplaceTempView("cleanedDF")
    val totalPopDF01 = spark.sql("select _c1, max(_c5), max(_c6), max(_c7), max(_c8), max(_c9), max(_c10), max(_c11), max(_c12) from cleanedDF where _cl1 in () group by _c1")
    val mapColumns = typedLit(regionsList)
    val dfFinal = totalPopDF01.withColumn("Region", coalesce(mapColumns($"_c1"), lit("")))


    dfFinal
  }

  def modifyDFcolumnsForFile02(df: DataFrame): DataFrame = {
    val cast_df = df.select(df.columns.map {
      case column@"_c0"=>col(column).cast("String").as(column)
      case column@"_c1"=>col(column).cast("String").as(column)
      case column => col(column).cast("Integer").as(column)
    }: _*)

    val cleaned_DF = cast_df.drop("_c0", "_c2", "_c3", "_c4")
    cleaned_DF.createOrReplaceTempView("cleanedDF")
    val totalPopDF01 = spark.sql("select _c1, max(_c6) joinedDF where _cl1 in () group by _c1")
    val mapColumns = typedLit(regionsList)
    val dfFinal = cleaned_DF.withColumn("Region", coalesce(mapColumns($"_c1"), lit("")))


    dfFinal
  }

  def uploadOptimizedData(df: DataFrame): Unit = {
    //df.write.mode("append").csv(s"s3a://${bucketName}/trendsDF/TrendDF.parquet")
    //df.coalesce(1).write.mode("append").csv(s"s3a://${bucketName}/rawOptimized/totalPop01_2000.parquet")
//    df.coalesce(1).write.mode("append").csv(s"s3a://${bucketName}/rawOptimized/totalPop01_2010.parquet")
//    df.coalesce(1).write.mode("append").csv(s"s3a://${bucketName}/rawOptimized/totalPop01_2020.parquet")
//
//    df.coalesce(1).write.mode("append").csv(s"s3a://${bucketName}/rawOptimized/totalPop02_2000.parquet")
//    df.coalesce(1).write.mode("append").csv(s"s3a://${bucketName}/rawOptimized/totalPop02_2010.parquet")
//    df.coalesce(1).write.mode("append").csv(s"s3a://${bucketName}/rawOptimized/totalPop02_2020.parquet")
  }

}
