/* NgasImport.scala */
// Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// DataFrames
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.functions._

// elasticsearch
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

object NgasImport {

  val accessRegex = """^(\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}:\d{2}\.\d{3}).*client_address=\(\'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*file\_id=(\d+)\_(\d+).*host=([^\s]+).*Thread\-(\d+)""".r    

  val threadRegex = """.*Thread\-(\d+)""".r
  val sizeRegex = """.*Size:\s(\d+).*Thread\-(\d+)""".r

  // Want to know the transfer rate of in-progress transfers and for which thread.
 // val transferRateRegex = """(?<=Transfer rate:)(\d+\.\d[MGKBmgkb]{2}\/s).*(?<=Thread-)(\d+)""".r

  val ipRegex = """.*HTTP reply sent to: \(\'([^\']+).*Thread\-(\d+)""".r

      case class Access(date: String, ip: String, obsId: Int, obsDate: String, host: String, thread: Int)
      case class IpThread(thread: Int, ip: String)
      case class SizeThread(thread: Int, size: Int)
      case class Thread(thread: Int)

    def main(args: Array[String]) = {
      val logFile = "file:///home/damien/project/ngaslogs-fe1/*.nglog"
      val conf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("NGAS Log Importer")
        .set("spark.executor.memory", "2g")
        .set("es.index.auto.create", "true")
      val sc = new SparkContext(conf)

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // setup data types

      // retrieve
      val accesses = sc.textFile(logFile)
        .filter(line => line.contains("|RETRIEVE?"))
        .map(line => extractAccess(line))
        .toDF()

      val archives = sc.textFile(logFile)
        .filter(line => line.contains("Archive Push/Pull"))
        .map(line => extractSize(line))
        .toDF()

      // get access thread numbers
      val httpReplys = sc.textFile(logFile)
        .filter(line => line.contains("HTTP reply sent to:"))
        .map(line => extractIp(line))
        .toDF()

      // get redirect and internal call thread numbers
      val redirects = sc.textFile(logFile) 
        .filter(line => 
          (line.contains("NGAMS_INFO_REDIRECT") || line.contains("Successfully handled Archive")))
        .map(line => extractThread(line))
        .toDF()

      val dataReplys = sc.textFile(logFile)
        .filter(line => line.contains("Sending data back to requestor"))
        .map(line => extractSize(line))
        .toDF()

      val accessesClean = accesses
        // remove redirects
        .join(redirects, accesses("thread") === redirects("thread"), "left_outer")
        // ensure threads with success messages
        .join(dataReplys, accesses("thread") === dataReplys("thread"), "inner")
        .saveToEs("spark/dftest", Map("es.mapping.id" -> "date"))

      sc.stop()
    }
    
    def extractThread(line: String): Thread = {
      threadRegex.findFirstIn(line) match {
        case Some(threadRegex(thread)) => new Thread(thread.toInt)
      }
    }

    def extractAccess(line: String): Access = {
      accessRegex.findFirstIn(line) match {
        case Some(accessRegex(date, ip, obsId, obsDate, host, thread)) => new Access(date, ip, obsId.toInt, obsDate, host, thread.toInt)
        case Some(accessRegex(date, obsId, obsDate, host, thread)) => new Access(date, "", obsId.toInt, obsDate, host, thread.toInt)
      }
    }

    def extractIp(line: String): IpThread = {
      ipRegex.findFirstIn(line) match {
        case Some(ipRegex(ip, thread)) =>
          new IpThread(thread.toInt, ip)
      }
    }

    def extractSize(line: String): SizeThread = {
      sizeRegex.findFirstIn(line) match {
        case Some(sizeRegex(size, thread)) =>
          new SizeThread(thread.toInt, size.toInt)
      }
    }
}
