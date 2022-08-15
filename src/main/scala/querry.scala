import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hive.ql.exec.spark.session

object question3_2000 {
    def main(args: Array[String]): Unit ={
        val spark = SparkSession
        .builder()
        .appName("Census")
        .config("spark.master", "local")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
        

        val df =spark.read.csv("src/main/input/output00.csv").toDF(
            "stateID", "regionID", "state", "Total_Population", "White", "African", "Indian", "Asian", "Native-Hawaiian/Pacific-Islander", "Others", "Hispanic_or_Latino"
        )

        df.createOrReplaceTempView("population")

        val sqlDF = spark.sql("SELECT SUM(CAST(Total_Population AS INT)) AS 2000_popbyregion, regionID FROM population GROUP BY regionID ORDER BY regionID ")
        sqlDF.show

        sqlDF.coalesce(1).write.option("header",true).csv("src/output/Q3_2000")

    }
}
object question3_2010 {
    def main(args: Array[String]): Unit ={
        val spark = SparkSession
        .builder()
        .appName("Census")
        .config("spark.master", "local")
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val df =spark.read.csv("src/main/input/output10.csv").toDF(
            "stateID", "regionID", "state", "Total_Population", "White", "African", "Indian", "Asian", "Native-Hawaiian/Pacific-Islander", "Others", "Hispanic_or_Latino"
        )

        df.createOrReplaceTempView("population")

        val sqlDF = spark.sql("SELECT SUM(CAST(Total_Population AS INT)) AS 2010_popbyregion, regionID FROM population GROUP BY regionID ORDER BY regionID ")
        sqlDF.show

        sqlDF.coalesce(1).write.option("header",true).csv("src/output/Q3_2010")
    }
}

object question3_2020 {
    def main(args: Array[String]): Unit ={
        val spark = SparkSession
        .builder()
        .appName("Census")
        .config("spark.master", "local")
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val df =spark.read.csv("src/main/input/output20.csv").toDF(
            "stateID", "regionID", "state", "Total_Population", "White", "African", "Indian", "Asian", "Native-Hawaiian/Pacific-Islander", "Others", "Hispanic_or_Latino"
        )

        df.createOrReplaceTempView("population")

        val sqlDF = spark.sql("SELECT SUM(CAST(Total_Population AS INT)) AS 2020_popbyregion, regionID FROM population GROUP BY regionID ORDER BY regionID ")
        sqlDF.show

        sqlDF.coalesce(1).write.option("header",true).csv("src/output/Q3_2020")
    }
}

object question3 {
    def main(args: Array[String]): Unit ={
        val spark = SparkSession
        .builder()
        .appName("Census")
        .config("spark.master", "local")
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

    val df00 =spark.read.csv("src/output00.csv").toDF(
    "stateID", "regionID", "state", "Total_Population", "White", "African", "Indian", "Asian", "Native-Hawaiian/Pacific-Islander", "Others", "Hispanic_or_Latino"
    )
    val df10 =spark.read.csv("src/output10.csv").toDF(
    "stateID", "regionID", "state", "Total_Population", "White", "African", "Indian", "Asian", "Native-Hawaiian/Pacific-Islander", "Others", "Hispanic_or_Latino"
    )
    val df20 =spark.read.csv("src/output20.csv").toDF(
    "stateID", "regionID", "state", "Total_Population", "White", "African", "Indian", "Asian", "Native-Hawaiian/Pacific-Islander", "Others", "Hispanic_or_Latino"
    )

    df00.createOrReplaceTempView("census00")
    df10.createOrReplaceTempView("census10")
    df20.createOrReplaceTempView("census20")

    val sqlDF = spark.sql("SELECT X.regionID AS Region, SUM(CAST(X.Total_Population AS INT)) AS 2000_population, SUM(CAST(Y.Total_Population AS INT)) AS 2010_population, SUM(CAST(Z.Total_Population AS INT)) AS 2020_population  " +
                          "FROM census00 X "+
                          "JOIN census10 Y ON X.stateID=Y.stateID "+
                          "JOIN census20 Z ON X.stateID=Z.stateID "+
                          "GROUP BY X.regionID "+
                          "ORDER BY X.regionID ")
    sqlDF.show

}
}