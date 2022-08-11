package example

import org.apache.spark.sql.SparkSession

object s3 {
    def main(args:Array[String]) {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[3]")
            .appName("Spark AWS S3")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val sc = spark.sparkContext

        val accessKey = "AKIA4OK5FKIYV3DQPT7K"
        val secretAccessKey = "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"
        val bucket = "revature-ajay-big-data-1452"

//         bucket name = revature-ajay-big-data-1452
// access key = AKIA4OK5FKIYV3DQPT7K
// secret key = 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

        // spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKey)
        // spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
        // spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
        spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        //OUTPUT FOLDERS FOR PARQUET 2020
        val OUTPUT_PARQUET_FOLDER_2020_1 = "./src/main/scala/output/parquet/2020/parquet_1_2020"
        val OUTPUT_PARQUET_FOLDER_2020_2 = "./src/main/scala/output/parquet/2020/parquet_2_2020"
        val OUTPUT_PARQUET_FOLDER_2020_3 = "./src/main/scala/output/parquet/2020/parquet_3_2020"
        //OUTPUT FOLDERS FOR PARQUET 2010
        val OUTPUT_PARQUET_FOLDER_2010_1 = "./src/main/scala/output/parquet/2010/parquet_1_2010"
        val OUTPUT_PARQUET_FOLDER_2010_2 = "./src/main/scala/output/parquet/2010/parquet_2_2010"
        //OUTPUT FOLDERS FOR PARQUET 2000
        val OUTPUT_PARQUET_FOLDER_2000_1 = "./src/main/scala/output/parquet/2000/parquet_1_2000"
        val OUTPUT_PARQUET_FOLDER_2000_2 = "./src/main/scala/output/parquet/2000/parquet_2_2000"
        val OUTPUT_PARQUET_FOLDER_2000_3 = "./src/main/scala/output/parquet/2000/parquet_3_2000"

        //INPUT FOLDERS IN S3 FOR 2020
        val INPUT_S3_FOLDER_2020_1 = "census/2020/census_2020_1"
        val INPUT_S3_FOLDER_2020_2 = "census/2020/census_2020_2"
        val INPUT_S3_FOLDER_2020_3 = "census/2020/census_2020_3"

        //INPUT FOLDERS IN S3 FOR 2010
        val INPUT_S3_FOLDER_2010_1 = "census/2010/census_2010_1"
        val INPUT_S3_FOLDER_2010_2 = "census/2010/census_2010_2"
      
        //INPUT FOLDERS IN S3 FOR 2000
        val INPUT_S3_FOLDER_2000_1 = "census/2000/census_2000_1"
        val INPUT_S3_FOLDER_2000_2 = "census/2000/census_2000_2"
        val INPUT_S3_FOLDER_2000_3 = "census/2000/census_2000_3"

    //     //FUNCTION THAT SAVE FILE TO DF AND UPLOAD  IT TO S3
    //     def uploadParuetToS3(year:Integer, fileNum:Integer): Unit = {

    //         val outputFolder = s"./src/main/scala/output/parquet/${year}/parquet_${fileNum}_${year}"
    //         //val inputFolder = s"census/${year}/census_${year}_${fileNum}"

    //         //save parquet file to df
    //         val df = spark.read.parquet(outputFolder)

    //         //Write parquet file into s3
    //         println("Writing to S3...")
    //         df.write.format("parquet")
    //             .mode("Overwrite")
    //             .save(s"s3a://${bucket}/census/${year}/census_${year}_${fileNum}")
    //     }


    //    //2020 YEAR======================================================================================
    //    //parquet_2020_1=================================================================================
    //     uploadParuetToS3(2020,1)
    //     println("1 DONE")
    //     //parquet_2020_2=================================================================================
    //     uploadParuetToS3(2020,2)
    //      println("2 DONE")
    //     //parquet_2020_3=================================================================================
    //     uploadParuetToS3(2020,3)
    //      println("3 DONE")

    //     //2010 YEAR=====================================================================================
    //     //parquet_2010_1=================================================================================
    //     uploadParuetToS3(2010,1)
    //      println("1 DONE")
    //     //parquet_2010_2=================================================================================
    //     uploadParuetToS3(2010,2)
    //      println("2 DONE")

    //     //2020 YEAR======================================================================================
    //    //parquet_2020_1=================================================================================
    //     uploadParuetToS3(2000,1)
    //      println("1 DONE")
    //     //parquet_2020_2=================================================================================
    //     uploadParuetToS3(2000,2)
    //      println("1 DONE")
    //     //parquet_2020_3=================================================================================
    //     uploadParuetToS3(2000,3)
    //      println("1 DONE")
        


      
        // Read parquet from s3
        // val parquet_1_2020_s3 = spark.read.parquet(s"s3a://${bucket}/census_2020_1")
        // val lineCount = parquet_1_2020_s3.count()
        // println(lineCount)

        // //parquet_2020_2===============================================================================
        //Save parquet file to df
        val parquet_2_2020 = spark.read.parquet("./src/main/scala/output/parquet/2020/parquet_2_2020")

        //Write parquet file into s3
        println("Writing to S3...")
        parquet_2_2020.write.format("parquet")
            .mode("Overwrite")
            .save(s"s3a://${bucket}/census_2020_2")
        
        // Read parquet from s3
        val parquet_2_2020_s3 = spark.read.parquet(s"s3a://${bucket}/census_2020_2")
        println("DONe")

        // //parquet_2020_3===============================================================================
        // //Save parquet file to df
        // val parquet_3_2020 = spark.read.parquet("./src/main/scala/output/parquet/2020/parquet_3_2020")

        // //Write parquet file into s3
        // println("Writing to S3...")
        // parquet_3_2020.write.format("parquet")
        //     .mode("Overwrite")
        //     .save(s"s3a://${bucket}/census_2020_3")
        
        // // Read parquet from s3
        // val parquet_3_2020_s3 = spark.read.parquet(s"s3a://${bucket}/census_2020_3")




    
            

        //  // 
        // // // Read CSV file into DataFrame.
        // // println("\nRead csv file into a DF...")
        // // val df1 = spark.read.csv(
        // // "file:///Users/AjayJayantilalSingal/Big Data/SparkSamples/resources/zipcodes.csv"
        // // )
        // // df1.printSchema()

        // // // Write the file to S3.
        // // println("Writing to S3...")
        // // df1.write.format("csv")
        // //     .mode("Overwrite")
        // //     .save(s"s3a://${bucket}/zipcodes")

        // // println("Reading from S3...")
        // // val file = s"s3a://${bucket}/zipcodes/part*.csv"
        // // val lines = sc.textFile(file)
        // // val lineCount = lines.count()
        // // println(lineCount)
        
    

        // // println("Write DF to S3 in parquet format...")
        // // df.write.mode("overwrite").parquet(s"s3a://${bucket}/people.parquet")    // Folder.
        // // println("Write DF to S3 in csv format...")
        // // df.write.mode("overwrite").csv(s"s3a://${bucket}/people.csv")       // Folder.

        // // println("Read parquet file from S3 into DF...")
        // // val parqDF = spark.read.parquet(s"s3a://${bucket}/people.parquet")
        // // println("Read csv file from S3 into DF...")
        // // val parqDF = spark.read.csv(s"s3a://${bucket}/people.csv")
        // // parqDF.printSchema()
        // // parqDF.show()


        // // println("##spark read text files from a directory into RDD")
        // // val rddFromFile = spark.sparkContext.textFile(s"s3a://${bucket}/people.csv/*.csv")
        // // println(rddFromFile.getClass)

        // // println("##Get data Using collect")
        // // rddFromFile.collect().foreach(f=>{
        // //     println(f)
        // //})        
    }
}
