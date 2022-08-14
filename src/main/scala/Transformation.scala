
//import com.amazonaws.auth.BasicAWSCredentials
//import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, AmazonS3ClientBuilder}
//import com.amazonaws.regions.Regions
//import com.amazonaws.services.s3.model.{GetObjectRequest, ObjectMetadata, PutObjectRequest}

import scala.collection.immutable._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import java.io.File

object Transformation {

  val spark:SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext
  spark.conf.set("spark.sql.broadcastTimeout", 36000)

  val bucketName = ""
  val bucketNameAWSJDK = ""
  val accessKey = ""
  val secretKey = ""

  val statesList = List("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
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
    //downloadParquet()
    spark.close()
  }

  def downloadParquet(): Unit = {
    val rawData1 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/census/2010/census_2010_1/")
    val rawData2 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/census/2010/census_2010_2/")
    rawData1.cache()
    rawData2.cache()
//    rawData1.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    rawData2.persist(StorageLevel.MEMORY_AND_DISK_SER)


    println("total population 01 data is ")
    val newOptimizedDF1 = modifyDFcolumnsForFile01(rawData1)

    println("hispanic population 01 data is ")
    val newOptimizedDF2 = modifyDFcolumnsForFile02(rawData2)

    val finalOptimizedDF = newOptimizedDF1.join(newOptimizedDF2, newOptimizedDF1("_c1") === newOptimizedDF2("_c1"), "inner").drop(newOptimizedDF2("_c1"))
      .select("_c1", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "Region").orderBy(newOptimizedDF1("_c1").asc)

    val dfToUpload = finalOptimizedDF.coalesce(1)

//    uploadOptimizedData(dfToUpload)
//    createLocalFile(dfToUpload)
  }

  def modifyDFcolumnsForFile01(df: DataFrame): DataFrame = {

    val cast_df = df.select(df.columns.map {
      case column@"_c0"=>col(column).cast("String").as(column)
      case column@"_c1"=>col(column).cast("String").as(column)
      case column => col(column).cast("Integer").as(column)
    }: _*)

    val cleaned_DF1 = cast_df.drop("_c0", "_c2", "_c3", "_c4", "_c6")
    cleaned_DF1.createOrReplaceTempView("cleanedDF1")
    val totalPopDF01 = spark.sql(s"select _c1, max(_c5) as _c5, max(_c7) as _c7, max(_c8) as _c8, max(_c9) as _c9, max(_c10) as _c10, max(_c11) as _c11, max(_c12) as _c12 from cleanedDF1 group by _c1 order by _c1 asc")

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

    val cleaned_DF2 = cast_df.drop("_c0", "_c2", "_c3", "_c4")
    cleaned_DF2.createOrReplaceTempView("cleanedDF2")
    val totalPopDF01 = spark.sql("select _c1, max(_c6) as _c6 from cleanedDF2 group by _c1")

    totalPopDF01
  }

  def createLocalFile(df1: DataFrame): Unit = {
    val df2 = df1.coalesce(1)
//    df2.write.mode("overwrite").parquet("resources/totalPop2000")
//    df2.write.mode("overwrite").parquet("resources/totalPop2010")
//    df2.write.mode("overwrite").parquet("resources/totalPop2020")
  }

  def uploadOptimizedData(df: DataFrame): Unit = {
//    df.coalesce(1).write.parquet(s"s3a://${bucketName}/optimized/2000/total_pop_2000_01")
//        df.write.parquet(s"s3a://${bucketName}/optimized/2000/total_pop_2000")
//    df.write.mode("overwrite").format("parquet").save(s"s3a://${bucketName}/optimized/2000/total_pop_2000")
  }

  def uploadFileThroughAWSJDK(): Unit = {
//      val fileToUpload = new File("resources/totalPop2000_1/total_pop_2000.parquet")
//      val clientRegion = Regions.US_EAST_1
//      val s3Client = AmazonS3ClientBuilder.standard.withRegion(clientRegion).build
////    Upload a text string as a new object.
//      s3Client.putObject(bucketNameAWSJDK, "total_pop_2000_01", fileToUpload)
  }
}
