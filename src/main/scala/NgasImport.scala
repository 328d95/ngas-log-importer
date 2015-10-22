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
      val linesRaw = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) =>
        // get file name from input split's file split object and hash it

        val fileHash = inputSplit.asInstanceOf[FileSplit].getPath.toString.hashCode.toString

        // iterate through lines in the input split and append the hash
        iterator
          .filter(splitAndLine => 
            (splitAndLine._2.toString).contains("[INFO]") &&
            (splitAndLine._2.toString).contains("Thread-"))
          .map(splitAndLine => splitAndLine._2+fileHash)
      }

      /* End adaptation */

      //////////////
      // ACCESSES //
      //////////////

      val linesAccesses = linesRaw.filter(line =>
        line.contains("NGAMS_INFO_REDIRECT") || 
        line.contains("path=|RETRIEVE?") ||
        line.contains("Sending data back to requestor") ||
        line.contains("Located suitable file for request"))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
        

 //     val transferRate = lines
 //       .filter(line => line.contains("Total time for handling request"))
 //       .map(line => logParser.extractTransfer(line))
 //       .toDF()

      val hosts = linesAccesses
        .filter(line => line.contains("Located suitable file for request"))
        .map(line => logParser.extractHost(line))
        .toDF()

      val redirects = linesAccesses 
        .filter(line => line.contains("NGAMS_INFO_REDIRECT"))
        .map(line => logParser.extractThread(line))
        .toDF()

      val dataReplys = linesAccesses
        .filter(line => line.contains("Sending data back to requestor"))
        .map(line => logParser.extractSize(line))
        .toDF()

      val accesses = linesAccesses
        .filter(line => line.contains("path=|RETRIEVE?"))
        .map(line => logParser.extractAccess(line))
        .toDF()

      val accessesClean = accesses
        .where(accesses("accessThread") !== "") // remove failed matches
        .join(redirects, accesses("accessThread") === redirects("thread"), "left_outer")
        .join(dataReplys, accesses("accessThread") === dataReplys("sizeThread"), "inner")
        .select("date", "ip", "host", "size", "file", "obsId", "obsDate", "accessThread")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

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

      linesAccesses.unpersist(false)

      /////////////
      // INGESTS //
      /////////////

      val linesIngests = linesRaw.filter(line =>
        line.contains("path=|QARCHIVE|") ||
        line.contains("Archive Push/Pull") ||
        line.contains("HTTP reply sent to:") ||
        line.contains("Successfully handled Archive"))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      val ingestSizes = linesIngests
        .filter(line => line.contains("Archive Push/Pull"))
        .map(line => logParser.extractSize(line))
        .toDF()

      val successfulIngests = linesIngests
        .filter(line => line.contains("Successfully handled Archive"))
        .map(line => logParser.extractFile(line))
        .toDF()

      val ingestIps = linesIngests
        .filter(line => line.contains("HTTP reply sent to:"))
        .map(line => logParser.extractIp(line))
        .toDF()

      val ingests = linesIngests
        .filter(line => line.contains("path=|QARCHIVE|"))
        .map(line => logParser.extractIngest(line))
        .toDF()

      val ingestsClean = ingests
        .where(ingests("ingestThread") !== "") // remove failed matches
        // keep only successful and get file data
        .join(successfulIngests, ingests("ingestThread") === successfulIngests("fileThread"), "inner")
        //get content size
        .join(ingestSizes, ingests("ingestThread") === ingestSizes("sizeThread"), "left_outer")
        .dropDuplicates(Seq("file"))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      val ingestsFull = ingestsClean
        .where(ingestsClean("ip") !== "")   
        .select("date", "ip", "size", "file", "obsId", "obsDate")
        .saveToEs("ngas/ingest", Map("es.mapping.id" -> "date"))

      val iplessIngests = ingestsClean
        .where(ingestsClean("ip") === "")
        .join(ingestIps, ingestsClean("fileThread") === ingestIps("ipThread"), "left")
        .select(//"date", "ip", "host", "size", "file", "obsId", "obsDate")
          ingestsClean("date"), ingestIps("ip"), ingestsClean("size"), 
          ingestsClean("file"), ingestsClean("obsId"), ingestsClean("obsDate"))
        .saveToEs("ngas/ingest", Map("es.mapping.id" -> "date"))

      linesIngests.unpersist(false)

      sc.stop()
    }


}
