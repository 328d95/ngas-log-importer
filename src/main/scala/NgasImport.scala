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
import files.modFiles

object NgasImport {

  val accessRegex = """^(\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}:\d{2}\.\d{3}).*client_address=\(\'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*file\_id=((\d+)\_(\d+)[^\|]+).*host=([^\s]+).*Thread\-(\d+)""".r    

  val threadRegex = """.*Thread\-(\d+)""".r
  val sizeRegex = """.*Size:\s(\d+).*Thread\-(\d+)""".r
  val ingestRegex = """^(\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}:\d{2}\.\d{3}).*client_address=\(\'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*host=([^\s]+).*content-length=(\d+).*filename="((\d+)_(\d+)[^\"]+).*Thread\-(\d+)""".r

  // Want to know the transfer rate of in-progress transfers and for which thread.
 // val transferRateRegex = """(?<=Transfer rate:)(\d+\.\d[MGKBmgkb]{2}\/s).*(?<=Thread-)(\d+)""".r

  val ipRegex = """.*HTTP reply sent to: \(\'([^\']+).*Thread\-(\d+)""".r

    // parsing types
    case class Access(date: String, ip: String, file: String, obsId: Long, obsDate: String, host: String, thread: Long)
    case class IpThread(ip: String, thread: Long)
    case class SizeThread(size: Long, thread: Long)
    case class Thread(thread: Long)
    case class Ingest(date: String, ip: String, host: String, size: Long, file: String, obsId: Long, obsDate: String, thread: Long)

    // args
    val logDir = "/home/damien/project/ngaslogs-fe1"
    val modDate = "1411353703"

    // setup
    val partitions = 30
    val modFiles = new modFiles
    val conf = new SparkConf()
      .setMaster("local[12]")
      .setAppName("NGAS Log Importer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def main(args: Array[String]) = {
      
      val lines = sc.textFile(modFiles.dirAfter(logDir, modDate))
        .filter(line => 
          line.contains("path=|QARCHIVE|") ||
          line.contains("path=|RETRIEVE?") ||
          //line.contains("Archive Push/Pull") ||
          line.contains("HTTP reply sent to:") ||
          line.contains("NGAMS_INFO_REDIRECT") || 
          line.contains("Successfully handled Archive") ||
          line.contains("Sending data back to requestor"))
        .coalesce(partitions)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
  
      val ingests = lines
        .filter(line => line.contains("path=|QARCHIVE|"))
        .map(line => extractIngest(line))
        .toDF()

      val accesses = lines
        .filter(line => line.contains("path=|RETRIEVE?"))
        .map(line => extractAccess(line))
        .toDF()

//      val archives = lines 
//        .filter(line => line.contains("Archive Push/Pull"))
//        .map(line => extractSize(line))
//        .toDF()

      // get ingest ip addresses
      val httpReplys = lines
        .filter(line => line.contains("HTTP reply sent to:"))
        .map(line => extractIp(line))
        .toDF()

      // get redirect and internal call thread numbers
      val redirects = lines 
        .filter(line => line.contains("NGAMS_INFO_REDIRECT"))
        .map(line => extractThread(line))
        .toDF()

      val handledArchives = lines
        .filter(line => line.contains("Successfully handled Archive"))
        .map(line => extractThread(line))
        .toDF()

      val dataReplys = lines
        .filter(line => line.contains("Sending data back to requestor"))
        .map(line => extractSize(line))
        .toDF()

      val accessesClean = accesses
        // remove failed matches
        .filter(accesses("thread") > 0)
        .join(redirects, accesses("thread") === redirects("thread"), "left_outer")
        .join(dataReplys, accesses("thread") === dataReplys("thread"), "inner")
        .saveToEs("ngas/access", Map("es.mapping.id" -> "date"))

      val ingestsClean = ingests
        // remove failed matches
        .filter(ingests("thread") > 0)
        .join(handledArchives, ingests("thread") === handledArchives("thread"), "inner")
        .dropDuplicates(Seq("file"))
        .saveToEs("ngas/ingest", Map("es.mapping.id" -> "date"))

      sc.stop()
    }

    def extractThread(line: String): Thread = {
      threadRegex.findFirstIn(line) match {
        case Some(threadRegex(thread)) => Thread(thread.toLong)
        case _ => Thread(0)
      }
    }

    def extractAccess(line: String): Access = {
      accessRegex.findFirstIn(line) match {
        case Some(accessRegex(date, ip, file, obsId, obsDate, host, thread)) =>
          Access(date, ip, file, obsId.toLong, obsDate, host, thread.toLong)
        case Some(accessRegex(date, file, obsId, obsDate, host, thread)) => 
          Access(date, "", file, obsId.toLong, obsDate, host, thread.toLong)
        case _ => Access("", "", "", 0, "", "", 0)
      }
    }

    def extractIp(line: String): IpThread = {
      ipRegex.findFirstIn(line) match {
        case Some(ipRegex(ip, thread)) => IpThread(ip, thread.toLong)
        case _ => IpThread("", 0)
      }  
    }

    def extractSize(line: String): SizeThread = {
      sizeRegex.findFirstIn(line) match {
        case Some(sizeRegex(size, thread)) => SizeThread(size.toLong, thread.toLong)
        case _ => SizeThread(0, 0)
      }
    }

  def extractIngest(line: String): Ingest = {
    ingestRegex.findFirstIn(line) match { 
      case Some(ingestRegex(date, ip, host, size, file, obsId, obsDate, thread)) =>
        Ingest(date, ip, host, size.toLong, file, obsId.toLong, obsDate, thread.toLong)
      case _ => Ingest("", "", "", 0, "", 0, "", 0)
    }
  }

}
