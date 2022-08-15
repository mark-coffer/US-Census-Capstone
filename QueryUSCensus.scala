package example


import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


object QueryCensus{
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession
            .builder()
            .master("local")
            .appName("US Census Query - Query Census info")
            .getOrCreate()
        import spark.implicits._ 
        spark.sparkContext.setLogLevel("ERROR")

        println("Create DF from output00.csv")
       // Create DF from output00.csv
        val data = spark.read.csv("C:/Users/sljac/Desktop/Retraining/Census_Project/output00.csv")
        val column = Seq("stateID","regionID", "state","totalPop", "White", "African", "Indian", "Asian", "NativeHawaiian/PacificIslander", "OtherRaceAlone", "Hispanic/Latino")
        val df = data.toDF(column:_*)
        df.show()


        //Query the total pop 2000
        df.createOrReplaceTempView("states") 
        println("SELECT totalPop FROM states")
        val pop_2000 = spark.sql("SELECT SUM(totalPop) AS `2000 US Total_Pop` FROM states").toDF()
        pop_2000.write.option("header", true).csv("output/2000_Pop")



        println("Create DF from output10.csv")
       // Create DF from output10.csv
        val data2 = spark.read.csv("C:/Users/sljac/Desktop/Retraining/Census_Project/output10.csv")
        val column2 = Seq("stateID","regionID", "state","totalPop", "White", "African", "Indian", "Asian", "NativeHawaiian/PacificIslander", "OtherRaceAlone", "Hispanic/Latino")
        val df2 = data2.toDF(column2:_*)
        df2.show()

        //Query the total pop 2010
        df2.createOrReplaceTempView("states10") 
        println("SELECT totalPop FROM states10")
        val pop_2010 = spark.sql("SELECT SUM(totalPop) AS `2000 US Total_Pop` FROM states").toDF()
        pop_2010.write.option("header", true).csv("output/2010_Pop")

        println("Create DF from output20.csv")
       // Create DF from output20.csv
        val data3 = spark.read.csv("C:/Users/sljac/Desktop/Retraining/Census_Project/output20.csv")
        val column3 = Seq("stateID","regionID", "state","totalPop", "White", "African", "Indian", "Asian", "NativeHawaiian/PacificIslander", "OtherRaceAlone", "Hispanic/Latino")
        val df3 = data3.toDF(column3:_*)
        df3.show()

        //Query the total pop 2020
        df3.createOrReplaceTempView("states20") 
        println("SELECT totalPop FROM states20")
       val pop_2020 = spark.sql("SELECT SUM(totalPop) AS `2000 US Total_Pop` FROM states").toDF()
        pop_2020.write.option("header", true).csv("output/2020_Pop")

            
    }


}