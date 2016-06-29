# ngas-log-importer
This Spark script processes NGAS logs and writes them to ElasticSearch.

The program can be compiled using [SBT](http://www.scala-sbt.org/) by running `sbt package` in the directory containing `build.sbt`, this will create a jar which can be passed to Spark.

#Getting started
1. First you will need to install and run [elasticsearch (I used 1.7.1)](https://www.elastic.co/downloads/elasticsearch)
2. Then you will need to add the NGAS index and it's mappings. `add-mappings.sh` should do this for you. If not, follow [this guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html) and adhere to the mappings json in `ngas-mapping.json`.
3. Now you need [Spark (I used 1.5.1 pre-built for hadoop 2.6)](http://spark.apache.org/downloads.html)
4. Install Scala (I used 2.10) -- look this up for whatever distro you're using.
5. Install [sbt (I used 0.13.11)](http://www.scala-sbt.org/0.13/docs/Setup.html)
6. Compile this project using `sbt package` from within this directory.
7. Edit `run.sh` to point to your Spark directory and your log directory -- this script is simply a wrapper for submitting to `spark-submit`. [Details are available here](http://spark.apache.org/docs/latest/submitting-applications.html). 
7. `run.sh` will download the spark-elasticsearch connector package for you.
7. The final three lines are the full path to the jar you created in step 5, the full path to the directory containing the logs and, optionally, a last processing date. 

Providing a last procesing date instructs the program to only process files modified after that date. Re-processing a partially modified file will NOT duplicate results.
`date` can be used to output the timestamp that is required for the program, using `$(date +%s -d "-1 day")` will tell the program to only process files that have been modified within the last day.
I recommend that a cronjob to move logs to AWS based on mod date should be setup and then a secondary cronjob be set up on AWS to then process the logs, perhaps 2 hours later.

#Configuration
Currently, in production, ElasticSearch is set to use 10GB of memory, therefore there is not much memory left for Spark, the server has 15GB total. Currently this program optimised to use a small amount of memory by caching only parsed results. However this has significant performance implications, we are forced to read the files many times. Multiple reads are okay for small directories, anything under 10GB should not take too long, however for larger directories of logs there will be significant slow down--O(x*n) relationship where x is > 1 and n is the number of log lines. To implement a faster but more memory hungry version you will need to cache `linesRaw`.

##Spark Setup
* Vary the number of executors to minimise unused RAM in the driver executor, more than two executors is likely more efficient - I didn't have time to test.
* When setting the number of executors remember that one executor will always be the driver.
* JVM heap allocation should ALWAYS BE BELOW 32GB above 32GB memory pointers will occupy twice as much memory.
* Spark allocates executor memory upon the start of the Spark script so if your program executor has 5GB you will need 5GB free when you start your program because Spark will assume this memory is free and will error when it attempts to allocate it.
* Spark threads should be larger than the number of cores for constant saturation.
* Running Spark using the [ec2 scripts](http://spark.apache.org/docs/latest/ec2-scripts.html) on an short lived cluster will make configuration easier as you will not have to worry about memory issues that occur when you run Spark alongside the very memory hungry ElasticSearch.

**Using a cluster of smaller machines (eg aws micro instances)**
* The current build method (getting packages on the fly) will cause slowdown (each server will have to procure the package, I do recal reading about a torrent algorithm being implemented but I don't remember the details--perhaps research that). To optimise, run large jobs and/or bake the dependencies into the jar that you submit--the concept is reffered to as an "Uber jar", sbt can do this.
* Network hops will be an issue. Dataframes implement network efficient join algorithms, but before going down this road I suggest reading about [data broadcasting](http://spark.apache.org/docs/latest/tuning.html#broadcasting-large-variables) and [shuffle optimisations](http://people.eecs.berkeley.edu/~kubitron/courses/cs262a-F13/projects/reports/project16_report.pdf).
* Static S3 will need to be optimised, I am not sure how to do this but minimising network hops should be your goal.


##ElasticSearch - Current configuration
* Configured with 10GB of memory
* Configuration is located in `/etc/elasticsearch` and `/etc/defaults/elasticsearch`
* Amount of heap allocated to caching has been altered from the default.
* ElasticSearch is installed as a service, regular service commands apply `sudo service elasticsearch <command>`
* add-mappings.sh adds the mappings required in ElasticSearch for the program.
* delete-index.sh, removes all data from elasticsearch BE CAREFUL
* Ubuntu page and memory limits have been removed so that ElasticSeach can hog memory
* Caching has not been optimised, caching some time period of data instead of letting ElasticSearch decide what to cache will reduce memory use.
* Marvel and Watcher should be setup to monitor the health of the ElasticSearch cluster.
* Watcher can also be used to implement monitoring of NGAS warnings and errors--to gain the most value from this Spark streaming should probably be used.

#Future Work
* Changing NGAS logs to a clearly defined structured format (eg JSON) - will make both the NGAS code and the parsing code more elegant - this is the first thing I would do.
* The Spark team released [project tungsten](https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html), this promises optimisations by creating Spark specific datatypes which more closely map to a pure bit representation of your data, they are more space efficient. Particularly `None` objects should be  investigated in this project.
* Possible optimisation: Parse everything at the start into one super set [case class](http://docs.scala-lang.org/tutorials/tour/case-classes.html) and then join using those, this will let you do single passes over the data and will be significantly more space efficient with project tungesten `None` objects. Be aware of the duplicate columns issue with Spark joins. When you join Spark will give you ALL columns even if they are named the same an the names will not be updated. Work arounds for this can be seen in the `hostlessAccesses` calculations where I use [select](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrame) (see the second declaration of select) with columns rather than just column names.
* Further investigation of datasources that support relationships may be useful for the implementation of project ID relationships (ask Chen) and for more in-depth statistics. Some contenders are [crate.io - still in alpha](crate.io), [NuoDB - proprietary](nuodb.com), [OrientDB - multi-modal db](orientdb.com) and [ArangoDB - multi-modal db](https://www.arangodb.com/). ElasticSearch was selected as a proof of concept first iteration and while normalisation of data is possible it is neither time nor space efficient. It would be wise to evaluate the increase in developer investment required to build and maintain a system based on one of these datasources compared to the value of the created graphs. This said I think that ElasticSearch is currently (Nov 2015) the best choice for non-relational or easily normalised data because of its mature ecosystem.

#Useful Links
Spark docs - http://spark.apache.org/docs/latest/
ElasticSearch <-> Spark Interface docs - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
ElasticSearch Guide - https://www.elastic.co/guide/en/elasticsearch/guide/master/index.html

