
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
    .appName("transformation")
    .getOrCreate()

  val sc = spark.sparkContext
  spark.conf.set("spark.sql.broadcastTimeout", 36000)

  val bucketName = ""
  val bucketNameAWSJDK = ""
  val accessKey = ""
  val secretKey = ""

  var rawData1, rawData2, rawData3, rawData4, rawData5, rawData6: DataFrame = null

  val yearList = List(2000, 2010, 2020)

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
    readOptimizedParquet()
//    spark.close()
//    downloadParquet()
//    spark.close()
  }


  //for future practice, I would create json file dedicated for website links, be able to extract values inside link and pass that as param for condition check

  def downloadParquet(): Unit = {

//    for (i <- 0 until 2) {
//      val thread = new Thread {
//        override def run: Unit = {
          rawData1 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/census/2000/census_2000_1/")
//          rawData2 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/census/2000/census_2000_2/")
          rawData3 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/census/2010/census_2010_1/")
//          rawData4 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/census/2010/census_2010_2/")
          rawData5 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/census/2020/census_2020_1/")
//          rawData6 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/census/2020/census_2020_2/")

          println("caching in progress")
          rawData1.persist(StorageLevel.MEMORY_AND_DISK_SER)
          rawData3.persist(StorageLevel.MEMORY_AND_DISK_SER)
          rawData5.persist(StorageLevel.MEMORY_AND_DISK_SER)
//        }
//      }
//      thread.start
//    }

    println("loading and transforming and uploading in progress...")
    val newOptimizedDF1 = modifyDFcolumnsForFile01(rawData1)
//    val newOptimizedDF2 = modifyDFcolumnsForFile02(rawData2)
    val newOptimizedDF3 = modifyDFcolumnsForFile01(rawData3)
//    val newOptimizedDF4 = modifyDFcolumnsForFile02(rawData4)
    val newOptimizedDF5 = modifyDFcolumnsForFile01(rawData5)
//    val newOptimizedDF6 = modifyDFcolumnsForFile02(rawData6)

//    val dfToUpload2000 = repartitionTable(newOptimizedDF1, newOptimizedDF2)
//    val dfToUpload2010 = repartitionTable(newOptimizedDF3, newOptimizedDF4)
//    val dfToUpload2020 = repartitionTable(newOptimizedDF5, newOptimizedDF6)
    val dfToUpload2000 = repartitionTable(newOptimizedDF1)
    val dfToUpload2010 = repartitionTable(newOptimizedDF3)
    val dfToUpload2020 = repartitionTable(newOptimizedDF5)

//    createLocalFile(dfToUpload2000, 2000)
//    createLocalFile(dfToUpload2010, 2010)
//    createLocalFile(dfToUpload2020, 2020)

    uploadOptimizedData(dfToUpload2000, 2000)
    uploadOptimizedData(dfToUpload2010, 2010)
    uploadOptimizedData(dfToUpload2020, 2020)
  }

  def modifyDFcolumnsForFile01(df: DataFrame): DataFrame = {

    val cast_df = df.select(df.columns.map {
      case column@"_c0"=>col(column).cast("String").as(column)
      case column@"_c1"=>col(column).cast("String").as(column)
      case column => col(column).cast("Integer").as(column)
    }: _*)

//    val cleaned_DF1 = cast_df.drop("_c0", "_c2", "_c3", "_c4", "_c6")
    val cleaned_DF1 = cast_df.drop("_c0", "_c2", "_c3", "_c6")
    cleaned_DF1.createOrReplaceTempView("cleanedDF1")
//    val totalPopDF01 = spark.sql(s"select _c1, _c4, max(_c5) as _c5, max(_c7) as _c7, max(_c8) as _c8, max(_c9) as _c9, max(_c10) as _c10, max(_c11) as _c11, max(_c12) as _c12, _c77 from cleanedDF1 group by _c1 order by _c1 asc")
    val totalPopDF01 = spark.sql(s"select _c1, _c4, _c5, _c7, _c8, _c9, _c10, _c11, _c12, _c77 from cleanedDF1 where _c4 = 1 group by _c1, _c4, _c5, _c7, _c8, _c9, _c10, _c11, _c12, _c77 order by _c1 asc")

    val mapColumns = typedLit(regionsList)
    val dfFinal = totalPopDF01.withColumn("Region", coalesce(mapColumns($"_c1"), lit("")))

    dfFinal
  }

  def modifyDFcolumnsForFile02(df: DataFrame): DataFrame = {
//    val cast_df = df.select(df.columns.map {
//      case column@"_c0"=>col(column).cast("String").as(column)
//      case column@"_c1"=>col(column).cast("String").as(column)
//      case column => col(column).cast("Integer").as(column)
//    }: _*)
//
//    val cleaned_DF2 = cast_df.drop("_c0", "_c2", "_c3", "_c4")
//    cleaned_DF2.createOrReplaceTempView("cleanedDF2")
//    val totalPopDF01 = spark.sql("select _c1, max(_c6) as _c6 from cleanedDF2 group by _c1")
//
//    totalPopDF01
    df
  }

  def repartitionTable(df1: DataFrame): DataFrame = {
    val df3 = df1.drop("_c4")
      .select("_c1","_c5", "_c77", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "Region").orderBy(df1("_c1").asc)
    val df4 = df3.coalesce(1)
    df4
  }

  def repartitionTable(df1: DataFrame, df2: DataFrame): DataFrame = {
//    val df3 = df1.join(df2, df1("_c1") === df2("_c1"), "inner").drop(df2("_c1"))
//      .select("_c1", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "Region").orderBy(df1("_c1").asc)
    val df3 = df1.join(df2, df1("_c1") === df2("_c1"), "inner").drop(df2("_c1"))
      .select("_c1", "_c5", "_c77", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "Region").orderBy(df1("_c1").asc)
        val df4 = df3.coalesce(1)
    df4
  }

  def createLocalFile(df: DataFrame, year: Int): Unit = {
//    for (i <- 0 until 3) {
//      val thread = new Thread {
//        override def run: Unit = {
//          if (yearList.contains(year)) {
//            df.write.mode("overwrite").parquet(s"resources/totalPop${year}")
//          }
//          else
//            println("year " + year + " is not right format. Please try again")
//        }
//      }
//    }

    if (year == 2000)
    {
      //      df.write.mode("overwrite").parquet(resources/totalPop${year})
    }
    else if (year == 2010)
    {
      //      df.write.mode("overwrite").parquet(resources/totalPop${year})
    }
    else if (year == 2020)
    {
      //      df.write.mode("overwrite").parquet(resources/totalPop${year})
    }

//    df2.write.mode("overwrite").parquet("resources/totalPop2000")
//    df2.write.mode("overwrite").parquet("resources/totalPop2010")
//    df2.write.mode("overwrite").parquet("resources/totalPop2020")

    println("Successfully created")
  }

  def uploadOptimizedData(df: DataFrame, year: Int): Unit = {

//    if (yearList.contains(year)) {
//      df.write.mode("overwrite").parquet(s"s3a://${bucketName}/optimized/${year}/total_pop_${year}")
//    }
//    else
//      println("year " + year + " is not right format. Please try again")

    if (year == 2000)
    {
      df.write.mode("overwrite").parquet(s"s3a://${bucketName}/optimized/2000/total_pop_2000")
    }
    else if (year == 2010)
    {
      df.write.mode("overwrite").parquet(s"s3a://${bucketName}/optimized/2010/total_pop_2010")
    }
    else if (year == 2020)
    {
      df.write.mode("overwrite").parquet(s"s3a://${bucketName}/optimized/2020/total_pop_2020")
    }

    println("Successfully uploaded")
  }

  def uploadFileThroughAWSJDK(): Unit = {
//      val fileToUpload = new File("resources/totalPop2000_1/total_pop_2000.parquet")
//      val clientRegion = Regions.US_EAST_1
//      val s3Client = AmazonS3ClientBuilder.standard.withRegion(clientRegion).build
////    Upload a text string as a new object.
//      s3Client.putObject(bucketNameAWSJDK, "total_pop_2000_01", fileToUpload)
  }

  def terminateProcess(fileCount: Int): Unit = {
//    println("fileCount is " + fileCount)
//    spark.close()
//    sys.exit(0)
  }

  def readOptimizedParquet(): Unit = {
//    val optimizedData1 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/optimized/2000/total_pop_2000")
//    println(optimizedData1.select("_c1").count())
//    optimizedData1.show(52)
//
//    val optimizedData2 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/optimized/2010/total_pop_2010")
//    println(optimizedData2.select("_c1").count())
//    optimizedData2.show(52)
//
//    val optimizedData3 = spark.read.option("delimiter", ",").parquet(s"s3a://${bucketName}/optimized/2020/total_pop_2020")
//    println(optimizedData3.select("_c1").count())
//    optimizedData3.show(52)
  }
}
