import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object query5 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .appName("Capstone")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val columns = Seq("stateID", "regionID", "state",
      "totalPop", "White", "African", "Indian", "Asian",
      "NativeHawaiian_PacificIslander", "OtherRace",
      "HispanicLatino")
    val df00 = spark.read.csv("input//output00.csv").toDF(columns: _*)
    //df00.show(5)
    val df10 = spark.read.csv("input//output10.csv").toDF(columns: _*)
    //df10.show(5)
    val df20 = spark.read.csv("input//output20.csv").toDF(columns: _*)
    //df20.show(5)
    df00.createOrReplaceTempView("census00")
    df10.createOrReplaceTempView("census10")
    df20.createOrReplaceTempView("census20")

    print("Please enter an abbreviation of state of choice: ")
    var state = scala.io.StdIn.readLine().toUpperCase()
    val q5 = spark.sql("SELECT census00.totalPop, census10.totalPop" +
          ",census20.totalPop FROM census00, census10, census20 " +
          s"where census00.state = '$state' and census10.state = '$state' and census20.state = '$state'").toDF(
      "2000_Pop", "2010_Pop", "2020_Pop")
    q5.show(false)

    q5.coalesce(1).write.csv("output//popdiffstate5.csv")

//    val q50 = spark.sql("select state, totalPop as 2000_Pop from census00 where state = 'TX'")
//    q50.show()
//    val q51 = spark.sql("select state, totalPop as 2010_Pop from census10 where state = 'TX'")
//    q51.show()
//    val q52 = spark.sql("select state, totalPop as 2020_Pop from census20 where state = 'TX'")
//    q52.show()
//    val x = q50.union(q51)
//    x.union(q52).show()
  }
}

