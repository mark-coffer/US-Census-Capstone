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

        // Create DF from output00.csv, out
        val data = spark.read.csv("output00.csv")
        val data2 = spark.read.csv("output10.csv")
        val data3 = spark.read.csv("output20.csv")

        val column = Seq("stateID","regionID", "state","totalPop", "White", "African", "Indian", "Asian", "NativeHawaiian_PacificIslander", "OtherRace", "Hispanic_Latino")
        val df = data.toDF(column:_*)
        val df2 = data2.toDF(column:_*)
        val df3 = data3.toDF(column:_*)
        
        //Create views for the csv files to query
        df.createOrReplaceTempView("states00")
        df2.createOrReplaceTempView("states10")
        df3.createOrReplaceTempView("states20")

        //Question 4: Aggregate the total population of these different categories in 2020
        val q4= spark.sql("select cast(sum(white) as INT) as White, " +
                    "cast(sum(african) as INT) as African, " +
                    "cast(sum(indian) as INT) as Indian, " +
                    "cast(sum(Asian) as INT) as Asian, " +
                    "cast(sum(NativeHawaiian_PacificIslander) as INT) as `Native Hawaiian and Pacific Islander`, " +
                    "cast(sum(OtherRace) as INT) as `Other Race Alone`, " +
                    "cast(sum(Hispanic_Latino) as INT) as `Hispanic/Latino`  " +
                    "from states20")
        q4.show()
        q4.write.option("header", "true").mode("overwrite").csv("input/q4/")


        //Question 7: Aggregate the total population in the states across the decades - Which states are growing the fastest?
        val q7 = spark.sql("select s0.state, s0.totalPop as `2000`, s1.totalPop as `2010`, (s1.totalPop - s0.totalPop ) as `diffence`, s1.totalPop as `2010a`, s2.totalPop as `2020`, (s2.totalPop - s1.totalPop ) as `diffence2` " +
                            "from ((states00 s0 " +
                            "JOIN states10 s1 ON s1.stateID = s0.stateID)" +
                            "JOIN states20 s2 ON s2.stateID = s1.stateID)" +
                            "order by `diffence2` desc limit 5").repartition(1)
        q7.show()
        q7.write.option("header", "true").mode("overwrite").csv("input/q7/")

        //Question 8: Aggregate the total population in the states across the decades - Are any states decreasing in population? 
        // 2000-2010
        val q8a = spark.sql("select s0.state, s0.totalPop as `2000`, s1.totalPop as `2010`, (s1.totalPop - s0.totalPop ) as `diffence between 2000 and 2010` " +
                            "from (states00 s0 " +
                            "JOIN states10 s1 ON s1.stateID = s0.stateID)" +
                            "order by `diffence between 2000 and 2010` desc").repartition(1)

        // 2010-2020
        val q8b = spark.sql("select s1.state, s1.totalPop as `2010`, s2.totalPop as `2020`, (s2.totalPop - s1.totalPop ) as `diffence between 2010 and 2020` " +
                            "from (states10 s1 " +
                            "JOIN states20 s2 ON s1.stateID = s2.stateID)" +
                            "order by `diffence between 2010 and 2020` desc").repartition(1)                            
                        
        q8a.show()
        q8b.show()
        q8a.write.option("header", "true").mode("overwrite").csv("input/q8a/")
        q8b.write.option("header", "true").mode("overwrite").csv("input/q8b/")
    }
}