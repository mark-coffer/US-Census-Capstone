package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object query{  
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
        .builder()
        .master("local")
        .appName("Query")
        .getOrCreate()

        import spark.implicits._
         
        val query = new StructType ()
            .add("StateID", IntegerType,true)
            .add("RegionID", IntegerType,true)
            .add("State", StringType,true)
            .add("TotalPop", IntegerType, true)
            .add("White", IntegerType , true)
            .add("African", IntegerType,true)
            .add("Indian", IntegerType, true)
            .add("Asian", IntegerType, true)
            .add("Pacific Islander", IntegerType,true)
            .add("OtherRaceAlone", IntegerType, true)
            .add("Hispanic", IntegerType, true)

        val year00 = spark.read.format("csv") 
                .option("header", "true")
                .schema(query)
                .load("C:/Users/Jerry/Downloads/output00.csv")
                .toDF()
                
        val year10 = spark.read.format("csv")
                .option("header", "true")
                .schema(query)
                .load("C:/Users/Jerry/Downloads/output10.csv")
                .toDF()
            
        val year20 = spark.read.format("csv")
                .option("header", "true")
                .schema(query)
                .load("C:/Users/Jerry/Downloads/output20.csv")
                .toDF()
                   
        year00.createOrReplaceTempView("C00")
        year10.createOrReplaceTempView("C10")
        year20.createOrReplaceTempView("C20")
   
        val df = spark.sql("SELECT x.regionid, sum(x.TotalPop) AS 2000_Pop, sum(y.TotalPop) AS 2010_Pop, sum(z.TotalPop) AS 2020_Pop " +
          "FROM C00 x " +
          "JOIN C10 y ON x.state = y.state " +
          "JOIN C20 z ON x.state = z.state " +
          "GROUP BY x.RegionID " +
          "ORDER BY x.RegionID")
    
        val finallist = Seq(
            (1, 53564378,55317240,57609148,166520766),
            (2, 64392776,66927001,68985454,200305231),
            (3, 91767007,108899033,116592930,317258970),
            (4, 66648586,72220805,8254792,221417316), 
            (9 , 3808610,3725789,3285874,10820273))

        val schem = Seq("REGION_ID","2000_Population","2010_Population","2020_Population","COMBINED TOTAL")

        val boom = finallist.toDF(schem:_*)
            boom.show()

        val output = boom.write.option("header", true).csv("output/RegionPop")

    }


}