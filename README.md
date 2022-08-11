# US-Census-Capstone
Analyze trends and provide visualizations using US Census Data pulled from 2000 - 2020.
This is a team project to showcase our knowledge and understanding acquired during training.

# Extraction from Census Steps:
-Download zip file from Census.gov for your chosen year and dataset
- After open your preferred IDE and create your build.sbt
- Inside your build.sbt ensure that you have jsoup inside your libarary dependencies
- After the build is imported you can begin to create your folders src/main/scala/(package_name)
- Inside your specified package_name folder you can create your scala file
- Inside your scala file you create your object(name)
- Inside your object you create a SparkSession and declare global variables for scala file w/urls you obtained from Census.gov
- Create input and output folders for the zip files obtained from urls
- Create functions for getting links, downloading links that work with path your have specified/created
- Unzip Census pl.files from the destination specified in your path
- Create a function to convert .pl files from census to csv files
- After you convert .p1 files to csv files convert the csv files into parquet files
- Make sure before you compile code/run that you have two folders added to src/main/scala/(package_name) called output and csv
- Lastly deploy the zip parquet files into the S3 bucket through spark scala application
