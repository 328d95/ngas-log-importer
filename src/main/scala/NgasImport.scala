/* NgasImport.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NgasImport {
    
    val ngasAccessRegex = """^(\d{4}\-\d{2}\-\d{2})T(\d{2}\:\d{2}:\d{2}\.\d{3})\s.*(?<=client_address=\(\')(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\'\,\s+(\d+).*(?<=file\_id=)(\d+)\_(\d+).*(?<=host=)([^\s]+).*(?<=user\-agent=)([^\s]+)""".r

    def main(args: Array[String]) = {
        val logFile = "file:///home/damien/project/ngaslogs-fe1/*.nglog"
        val conf = new SparkConf()
          .setMaster("local[4]")
          .setAppName("NGAS Log Importer")
          .set("spark.executor.memory", "2g")
        val sc = new SparkContext(conf)
        val accesses = sc.textFile(logFile).cache()
          .filter(line => line.contains("Handling HTTP request:"))
          .filter(line => line.contains("method=GET"))
          .map(line => extractValues(line))
        sc.stop()
    }

    def extractValues(line: String): (String, String, String, String, String, String, String, String) = {
      ngasAccessRegex.findFirstIn(line) match {
        case Some(ngasAccessRegex(date, time, ip, port, obsId, obsDate, host, agent)) =>
          (date, time, ip, port, obsId, obsDate, host, agent)
        case _ => (null, null, null, null, null, null, null, null)
      }
    }
}
