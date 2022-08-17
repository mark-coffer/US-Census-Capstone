import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SQLquery{

    def main(args:Array[String]) ={
    
        val spark: SparkSession = SparkSession
            .builder()
            .master("local")
            .appName("SQLquery")
            .getOrCreate()


        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark.sparkContext

        import spark.implicits._    //SQLImplicits

        // val accessKey = "############"
        // val secretAccessKey = "#########"
        // val bucket = "################"

        // spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
        // spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
        // spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        // Create DF from input parquet files
        //val parquetDF = spark.read.parquet(s"s3a://${bucket}/optimized/2020/total_pop_2020")
        val data00 = spark.read.parquet("total_pop_2000.parquet")
        val data10 = spark.read.parquet("total_pop_2010.parquet")
        val data = spark.read.parquet("total_pop_2020.parquet")

        val column = Seq("State","TotalPop", "Hispanic_Latino", "White", "African", "Indian", "Asian", "NativeHawaiian_PacificIslander", "OtherRace", "Region")
        val df0 = data00.toDF(column:_*)
        val df1 = data10.toDF(column:_*)
        val df = data.toDF(column:_*)
        
        //Create views for the csv files to query
        df.createOrReplaceTempView("states20")
        df0.createOrReplaceTempView("states00")
        df1.createOrReplaceTempView("states10")

        //Question 4: Aggregate the total population of these different categories in 2020
        val q4= spark.sql("select cast(sum(white) as INT) as White, " +
                    "cast(sum(african) as INT) as African, " +
                    "cast(sum(indian) as INT) as Indian, " +
                    "cast(sum(Asian) as INT) as Asian, " +
                    "cast(sum(NativeHawaiian_PacificIslander) as INT) as `Native Hawaiian and Pacific Islander`, " +
                    "cast(sum(OtherRace) as INT) as `Other Race Alone`" +
                    "from states20")
        q4.show()
        q4.write.option("header", "true").mode("overwrite").csv("input/q4/")

        //Question 4: Aggregate the total population of these different categories in 2010
        val q4a= spark.sql("select cast(sum(white) as INT) as White, " +
                    "cast(sum(african) as INT) as African, " +
                    "cast(sum(indian) as INT) as Indian, " +
                    "cast(sum(Asian) as INT) as Asian, " +
                    "cast(sum(NativeHawaiian_PacificIslander) as INT) as `Native Hawaiian and Pacific Islander`, " +
                    "cast(sum(OtherRace) as INT) as `Other Race Alone`" +
                    "from states10")
        q4a.show()
        q4a.write.option("header", "true").mode("overwrite").csv("input/q4b/")

        // //Question 4: Aggregate the total population of these different categories in 2000
        val q4b= spark.sql("select cast(sum(white) as INT) as White, " +
                    "cast(sum(african) as INT) as African, " +
                    "cast(sum(indian) as INT) as Indian, " +
                    "cast(sum(Asian) as INT) as Asian, " +
                    "cast(sum(NativeHawaiian_PacificIslander) as INT) as `Native Hawaiian and Pacific Islander`, " +
                    "cast(sum(OtherRace) as INT) as `Other Race Alone`" +
                    "from states00")
        q4b.show()
        q4b.write.option("header", "true").mode("overwrite").csv("input/q4a/")

        //Question 8: Aggregate the total population in the states across the decades - Are any states decreasing in population? 
        //2000-2010
        val q8a = spark.sql("select s0.state, s0.TotalPop as `2000`, s1.TotalPop as `2010`, (s1.totalPop - s0.totalPop ) as `diffence between 2000 and 2010` " +
                            "from (states00 s0 " +
                            "JOIN states10 s1 ON s1.state = s0.state)" +
                            "order by `diffence between 2000 and 2010` asc").repartition(1)

        // 2010-2020
        val q8b = spark.sql("select s1.state, s1.totalPop as `2010`, s2.totalPop as `2020`, (s2.totalPop - s1.totalPop ) as `diffence between 2010 and 2020` " +
                            "from (states10 s1 " +
                            "JOIN states20 s2 ON s1.state = s2.state)" +
                            "order by `diffence between 2010 and 2020` asc").repartition(1)                            
                        
        q8a.show()
        q8b.show()
        q8a.write.option("header", "true").mode("overwrite").csv("input/q8a/")
        q8b.write.option("header", "true").mode("overwrite").csv("input/q8b/")
    }
}