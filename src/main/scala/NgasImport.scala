/* NgasImport.scala */
// Spark
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

// DataFrames
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.functions._

// elasticsearch
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

// mod date code
import files.ModFiles

// parser
import parser.LogParser

// hadoopRDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.HadoopRDD

// exceptions
import scala.util.control.Exception._

object NgasImport {


  //val transferRateRegex = """.*\)\:\s(\d+\.\d+).*Transfer rate:(\d+\.\d+).*(?<=Thread-)(.*)""".r


    // setup
    val partitions = 30
    val modFiles = new ModFiles
    val logParser = new LogParser
    val conf = new SparkConf().setAppName("NGAS Log Importer")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def main(args: Array[String]) = {
  
      val modDir = args.length match {
        case 0 => throw new IllegalArgumentException("The first argument must be the path to the log directory.")
        case 1 => modFiles.logFiles(args(0))
        case 2 => modFiles.logFiles(args(0), args(1))
      }

      /* Adapted from http://themodernlife.github.io/scala/spark/hadoop/hdfs/2014/09/28/spark-input-filename/ */
      // Create the text file
      val text = 
          sc.hadoopFile(modDir, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], sc.defaultMinPartitions)

      // Cast to a HadoopRDD
      val hadoopRdd = text.asInstanceOf[HadoopRDD[LongWritable, Text]]
      val lines = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) =>
        // get file name from input split's file split object and hash it

        val fileHash = inputSplit.asInstanceOf[FileSplit].getPath.toString.hashCode.toString

        // iterate through lines in the input split and append the hash
        iterator
          .filter(splitAndLine => (splitAndLine._2.toString).contains("[INFO]"))
          .filter(splitAndLine => (splitAndLine._2.toString).contains("Thread-")) // get rid of janitor threads
          .map(splitAndLine => splitAndLine._2+fileHash)
      }

      /* End adaptation */

      //////////////
      // ACCESSES //
      //////////////

 //     val transferRate = lines
 //       .filter(line => line.contains("Total time for handling request"))
 //       .map(line => logParser.extractTransfer(line))
 //       .toDF()

      val hosts = lines
        .filter(line => line.contains("Located suitable file for request"))
        .map(line => logParser.extractHost(line))
        .toDF()
        .persist(StorageLevel.MEMORY_AND_DISK_SER) 

      val redirects = lines 
        .filter(line => line.contains("NGAMS_INFO_REDIRECT"))
        .map(line => logParser.extractThread(line))
        .toDF()
        .persist(StorageLevel.MEMORY_AND_DISK_SER) 

      val dataReplys = lines
        .filter(line => line.contains("Sending data back to requestor"))
        .map(line => logParser.extractSize(line))
        .toDF()
        .persist(StorageLevel.MEMORY_AND_DISK_SER) 

      val accesses = lines
        .filter(line => line.contains("path=|RETRIEVE?"))
        .map(line => logParser.extractAccess(line))
        .toDF()
        .persist(StorageLevel.MEMORY_AND_DISK_SER) 

      val accessesClean = accesses
        .where(accesses("accessThread") !== "") // remove failed matches
        .join(redirects, accesses("accessThread") === redirects("thread"), "left_outer")
        .join(dataReplys, accesses("accessThread") === dataReplys("sizeThread"), "inner")
        .select("date", "ip", "host", "size", "file", "obsId", "obsDate", "accessThread")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      accesses.unpersist
      redirects.unpersist
      dataReplys.unpersist

      val fullAccesses = accessesClean
        .where(accessesClean("host") !== "")
        .saveToEs("ngas/access", Map("es.mapping.id" -> "date"))

      // add hosts then save
      val hostlessAccesses = accessesClean
        .where(accessesClean("host") === "")
        .join(hosts, accessesClean("accessThread") === hosts("hostThread"), "left")
        .select(
          accessesClean("date"), accessesClean("ip"), hosts("host"), accessesClean("size"), 
          accessesClean("file"), accessesClean("obsId"), accessesClean("obsDate"))
        .saveToEs("ngas/access", Map("es.mapping.id" -> "date"))

      /////////////
      // INGESTS //
      /////////////

      val handledArchives = lines
        .filter(line => line.contains("Successfully handled Archive"))
        .map(line => logParser.extractThread(line))
        .toDF()
        .persist(StorageLevel.MEMORY_AND_DISK_SER) 

      val ips = lines
        .filter(line => line.contains("HTTP reply sent to:"))
        .map(line => logParser.extractIp(line))
        .toDF()
        .persist(StorageLevel.MEMORY_AND_DISK_SER) 

      val ingests = lines
        .filter(line => line.contains("path=|QARCHIVE|"))
        .map(line => logParser.extractIngest(line))
        .toDF()
        .persist(StorageLevel.MEMORY_AND_DISK_SER) 

      val ingestsClean = ingests
        .where(ingests("ingestThread") !== "") // remove failed matches
        .join(handledArchives, ingests("ingestThread") === handledArchives("thread"), "inner")
        .dropDuplicates(Seq("file"))
        .select("date", "ip", "host", "size", "file", "obsId", "obsDate", "thread")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      ingests.unpersist 
      handledArchives.unpersist
        
      val ingestsFull = ingestsClean
        .where(ingestsClean("ip") !== "")   
        .select("date", "ip", "host", "size", "file", "obsId", "obsDate")
        .saveToEs("ngas/ingest", Map("es.mapping.id" -> "date"))

      val iplessIngests = ingestsClean
        .where(ingestsClean("ip") === "")
        .join(ips, ingestsClean("thread") === ips("ipThread"), "left")
        .select(//"date", "ip", "host", "size", "file", "obsId", "obsDate")
          ingestsClean("date"), ips("ip"), ingestsClean("host"), ingestsClean("size"), 
          ingestsClean("file"), ingestsClean("obsId"), ingestsClean("obsDate"))
        .saveToEs("ngas/ingest", Map("es.mapping.id" -> "date"))

      sc.stop()
    }


}
