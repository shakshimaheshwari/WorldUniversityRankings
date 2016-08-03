# WorldUniversityRankings
The project aimed at analysing and interpretating the data-set provided by Kaggle to discover facts about top universities throughout the globe.

#Pre-requisites
```
$ Install JAVA
$ Install Virtual box
$ Install HADOOP
$ Install UNIX OS
```
#Procedure
```
$ Set up the HADOOP environment
$ Browse to the directory /usr/local/hadoop
$ Type in the following command to see if the USER directory already exists hdfs dfs -ls /
$ If exist create an input directory using **sudo mkdir /user/input** else create the **user** directory first 
and then create the input directory
$ Place the input file to the /user/input drectory using **hadoop fs -put <path to the input file> /user/input**
$ create a java file and write in the map reduce code to solve or analyze the given question in hand
$ execute the commands [https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html] to compile, create the jar and then execute the map-reduce.
$ After the output is created get the output to the local directory using the command  **hadoop fs -get <path to the output file> <path where you want to store the file>
```
# Data-set description
* **timesData.csv**:- This file consist of the information about the Universities like world_ranking, university_name, country, teaching, international, reserach, citations, income, total_score, num_students, studentrs_staff_rate, female_male, year where all these details are provided year wise from the year 2011 to year 2016
* **cwurData.csv**:- This file consist of information about the universities like world_ranking, institution_name, national_rank, quality_of_education, alumni_employment, quality_of_faculty, publications, influence, citations, broad_impact, patents, score, year where all these details are provided year wise from the year 2012 to year 2015

# Questions Analysed on the Dataset
* How many universities from each country were there in the top list over the period defined
* Each country had how many students enrolled every year collaboratively over all universities belonging to that country
* The number of international students enrolled in each university every year grouped on the basis of the university
* The quality of education rating that each university has across all the years grouped by university and year
* The quality of faculty rating that each university has across all the years
* The cumulative number of patents that each university has filed for over the period of five years arranged in descending order on the number of patents

# Deployment
Can be deployed to Amazon's EMR or Google Cloud
