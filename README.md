# ngas-log-importer
This Spark script processes NGAS logs and writes them to ElasticSearch.

The program can be compiled using [SBT](http://www.scala-sbt.org/) by running `sbt package`, this will create a jar which can be passed to Spark.

#Scripts
The scripts directory contains an example run script for a Spark program, this script will initialise Spark and load the ElasticSearch dependency.

The first three directives in the script deinfe the path to the Spark binary, the cluster name and number of cores to use, and the amount of RAM per executor.
Packages loads the ElasticSearch connector and conf tells Spark that ElasticSearch is located on localhost:9200.
Then you pass the executable, the log files (which can be a directory or a single file) and the last processing date.

The last processing date is used as a guide to minimise recomputation, overlap in the processing of files will NOT duplicate results.
`date` can be used to output the timestamp that is required for the program, using `$(date +%s -d "-1 day")` will tell the program to only process files that have been modified within the last day.
I recommend that a cronjob to move logs to AWS based on mod date should be setup and then a secondary cronjob be set up on AWS to then process the logs, perhaps 2 hours later.

#Configuration
Currently ElasticSearch is set to use 10GB of memory on the cluster, therefore there is not much memory left for Spark. Currently this program is setup to use a small amount of memory by caching only parsed results. However this has significant performance implications, we are forced to read the files many times in this situation. Multiple reads are okay for log directory sizes, anything under 10GB should not take too long, however for larger directories of logs there will be significant slow down--O(x*n) relationship where x is > 1 and n is the number of log lines. To implement a faster but more memory hungry version you will need to cache `linesRaw`.

##Spark Setup
* Vary number of executors to minimise the unused RAM in the driver executor, more than two executors is likely more efficient - I didn't have time to test.
* When setting the number of executors remember that one executor will always be the driver.
* Running Spark using the [ec2 scripts](http://spark.apache.org/docs/latest/ec2-scripts.html) on an short lived cluster will make configuration easier as you will not have to worry about out of memory issues that you have to worry about when running Spark on the same nodes as ElasticSearch.
** Doing this will likely involve in more dependency procurement costs (each run will have to get the ElasticSearch dependency again on static nodes the dependencies will be saved) so it will not be efficient for many small jobs run in sequence.
** Baking all of the dependencies into the jar is a possibility the concept is often reffered to as an "Uber jar", SBT has functionality for this.
** Network optimisation will become an issue with this setup, Dataframes implement efficient join algorithms however you should be aware.
** Static S3 will need to be optimised for this set up, I am not sure how to do this but minimising network hops should be your goal.
* JVM heap allocation should ALWAYS BE BELOW 32GB above 32GB memory pointers will occupy twice as much memory.
* Spark allocates executor memory upon the start of the Spark script so if your program executor has 5GB you will need 5GB free when you start your program because Spark will assume this memory is free and will error when it attempts to allocate it.
* Spark threads should be larger than the number of cores for constant saturation.

##ElasticSearch 
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
* During my project Spark released project tungsten investigation into optimisation of this code by using the now more memory efficient None objects in Scala should be investigated.
Parse everything at the start into one super set case class and then join using those. Be aware of the duplicate columns issue with Spark joins. When you join Spark will give you ALL columns even if they are named the same an the names will not be updated. Work arounds for this can be seen in the `hostlessAccesses` calculations where I use [select](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrame)(see the second declaration of select) with columns rather than just column names.
* Further investigation into datasources that support relationships will be useful for the implementation of project ID relationships and for more in-depth statistics. Some contenders are (crate.io - still in alpha)[crate.io], (NuoDB - proprietary)[nuodb.com] and (OrientDB - graph database)[orientdb.com]. ElasticSearch was selected as a proof of concept first iteration and while normalisation of data is possible it is neither time nor space efficient. It would be wise to evaluate the increase in developer investment required to build and maintain a system based on one of these datasources compared to the value of the created graphs. This said I think that ElasticSearch is currently (Nov 2015) the best choice for non-relational or easily normalised data because of its mature ecosystem.
** I would investigate in this order CrateDB (SQL syntax will be easier to maintain) -> OrientDB (probably best in class performance wise, graph interface may be more difficult to maintain) -> NuoDB (CrateDB and NuoDB seem to be basically the same thing but you have to pay for Nuo, then again the support may be worth it).

#Useful Links
Spark docs - http://spark.apache.org/docs/latest/
ElasticSearch <-> Spark Interface docs - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
ElasticSearch Guide - https://www.elastic.co/guide/en/elasticsearch/guide/master/index.html

