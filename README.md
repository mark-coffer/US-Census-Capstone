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

# Transformation
-Ensure all data is stored in parquet format, increasing processing speed
-Ensure data is clear and legibile, formatting where needed (ex: Changing Regions from numbers to Strings indicating exact value)
-Use elements such as caching, broadcast variables, multi-threading, and coalesce to condense data retrieved from the website.
-Work with the query team to create dataframes that fit the needs of the questions we are asking of the data.
-Results were a reduction in file size from 80 mb to 5 kb after all processes were complete.

# Query
The goal of the query team is to answer these questions:
1) Aggregate the total population of the United states in the given years
2) Aggregate the total population in the five US regions : the Northeast, Southwest, West, Southeast, and Midwest. Which is the most densely populated?
3) Aggregate the total population in the five US regions : the Northeast, Southwest, West, Southeast, and Midwest across the decades. Which regions are     growing the fastest?
4) The given census data is divided into the different race/ethnicities present in the United States. Aggregate the total population of these different categories
5) Show a trendline describing the population change of states between the different decades.
6) Show a trendline describing the population change of states between the different decades leading up to 2020 predicting the 2020 population, then compare to the actual result.
7) Aggregate the total population in the states across the decades. Which states are growing the fastest?
8) Aggregate the total population in the states across the decades. Are any states decreasing in population?

Finally, We must make predictions using the data at hand for the future.

# Visualizations
This team must work closely with the query team to create visual representations of all data to be presented. Focusing on engaging displays in formats befitting the data to be represented.
