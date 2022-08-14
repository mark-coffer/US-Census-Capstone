package main
import scala.language.postfixOps
import scala.collection.mutable.Map 
import collection.JavaConverters._
import sys.process._
import java.net.URL
import java.io.File
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import java.io.{FileInputStream, FileOutputStream}
import java.util.zip.ZipInputStream
import scala.util.control.Breaks._
import java.io.{ IOException, FileOutputStream, FileInputStream, File }
import java.util.zip.{ ZipEntry, ZipInputStream }
import org.apache.spark.sql.SparkSession



object project{
    def main(args:Array[String]):Unit ={

        val spark: SparkSession = SparkSession
            .builder()
            .master("local[3]")
            .appName("Spark S3")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val sc = spark.sparkContext

        //GLOBAL VARIABLES:
        val link2020 = "https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/"
        val link2010 = "https://www2.census.gov/programs-surveys/decennial/2010/data/01-Redistricting_File--PL_94-171/"
        val link2000 = "https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/"

        //OUTPUT AND INPUT FOLDER FOR 2020
        val zipsFolder2020 = "./src/main/scala/output/zips/zips_2020"
        val perlFolder2020: String = "./src/main/scala/output/csv/csv_2020";
        
        //OUTPUT AND INPUT FOLDER FOR 2010
        val zipsFolder2010 = "./src/main/scala/output/zips/zips_2010"
        val perlFolder2010: String = "./src/main/scala/output/csv/csv_2010";
        
        //OUTPUT AND INPUT FOLDER FOR 2000
        val zipsFolder2000 = "./src/main/scala/output/zips/zips_2000"
        val perlFolder2000: String = "./src/main/scala/output/csv/csv_2000";


        //FUNCTION THAT CONNECT TO EACH LINK AND CREATE A LIST OF LINKS TO ZIP FILES
        def getLinks(url: String, zipNum: Integer):List[String] = {
         
            val document: Document = Jsoup.connect(url).get()
            val states: Elements = document.select("tbody tr td a")
            val stateNames = for(state <- states.asScala) yield (state.text)
            val filteredStateNames=stateNames.filter(_.contains("/"))
            val links = for (urlZip<- filteredStateNames) yield {url+ urlZip}
           

            val zip_links = for (link <- links) yield Jsoup.connect(link).get()
            //alter eq nummerical value to get the desired element you want for the hyperlink 
            //2000: for 001zip use eq(5), for 002zip use eq6, for geozip use eq(7)
            //2010 : eq(5)
            //2020: eq(3)
            val mytags2 = for (hyperlink <- zip_links) yield hyperlink.select(s"tbody tr:eq(${zipNum}) td a")
            
            val testing = for (x <- mytags2) yield (x.text)
            val zipfiles = for (i <- Range(0,52)) yield links(i).concat(testing(i))
            val zips = zipfiles.toList
            println("SCRAPED LINKS WITH ZIPS FROM THE WEBSITE ======================")
            zips
            
            }

        //FUNCTION THAT LOAD ZIP FROM EACH LINK AND STORE IT IN SPECIFIED PATH
        def downloadFiles(downloadableFileLinks: List[String], path: String):Unit = { 
                downloadableFileLinks.map( downloadableLink => 
                {
                val urlObject = new URL(downloadableLink)
                val filePath = path + "/" + urlObject.getPath().replaceAll("/", "")
                urlObject #> new File(filePath)!!
                }
              )
                println("SAVED ZIPS TO FOLDER ================================================")
          }

        //println("ZIPS DOWNLOADED =============================================")
        
        //FUNCTION THAT UNZIP DOWNLOADED LINKS WITH ZIPS
        def unZipAll(downloadableFileLinks: List[String], path: String, outputFolder: String):Unit ={

             downloadableFileLinks.map( downloadableLink => {
                val urlObject = new URL(downloadableLink)
                val filePath = path + "/" + urlObject.getPath().replaceAll("/", "")

                //CALL FUNCTION THAT WILL UNZIP EACH ZIP BY ONE AND SAVE IT TO PATH
                unZipOne(filePath, outputFolder)

                }
            )
            println("UNZIPED ALL FILES AND SAVED IT TO FOLDER ===================================")
        }

        def unZipOne(zipFile: String, outputFolder: String): Unit = {

          val buffer = new Array[Byte](1024)

          try {

            //output directory
            val folder = new File(outputFolder);

            if (!folder.exists()) {
              folder.mkdir();
            }
        
            //zip file content
            val zis: ZipInputStream = new ZipInputStream(new FileInputStream(zipFile));
          
            //get the zipped file list entry
            var ze: ZipEntry = zis.getNextEntry();

            while (ze != null) {
              val fileName = ze.getName();
              //CONVERT .PL TO .CSV
              var csvName =""
              if (fileName.contains(".upl")){
                csvName=fileName.substring(0,fileName.length()-3)+ "csv"
              }else if (fileName.contains(".pl")){
                csvName=fileName.substring(0,fileName.length()-2)+ "csv"
              }
        
              val newFile = new File(outputFolder + File.separator +  csvName);
             
              System.out.println("file unzip : " + newFile.getAbsoluteFile());

              //create folders
              new File(newFile.getParent()).mkdirs();
              val fos = new FileOutputStream(newFile);
              var len: Int = zis.read(buffer);

              while (len > 0) {
                fos.write(buffer, 0, len)
                len = zis.read(buffer)
              }

              fos.close()
              ze = zis.getNextEntry()
            }

            zis.closeEntry()
            zis.close()

          } catch {
            case e: IOException => println("exception caught: " + e.getMessage)
          }
        }
        
        //RUN FUNCTIONS THAT WILL DOWNLOAD ZIPS, UNZIP IT AND SAVE IT AS CSV

        //2000 YEAR
        //val zipLinks2000 = getLinks(link2000, 5) //for 0001 files
        //val zipLinks2000 = getLinks(link2000, 6)   //for 0002 files
        //val zipLinks2000 = getLinks(link2000, 7)   //for geo files
        //downloadFiles(zipLinks2000, zipsFolder2000)
        //unZipAll(zipLinks2000, zipsFolder2000, perlFolder2000)

        //2010 YEAR
        //val zipLinks2010 = getLinks(link2010, 5)
        //downloadFiles(zipLinks2010, zipsFolder2010)
        //unZipAll(zipLinks2010, zipsFolder2010, perlFolder2010)

        //2020 YEAR
        val zipLinks2020 = getLinks(link2020, 3)
        //downloadFiles(zipLinks2020, zipsFolder2020)
        unZipAll(zipLinks2020, zipsFolder2020, perlFolder2020)
        

        
    
    
   
}
}