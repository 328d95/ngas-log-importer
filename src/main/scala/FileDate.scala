package files
import java.io.File

class modFiles {
    def dirAfter(directory: String, modTime: String): String = {
      // get list of all log files
      val files = new java.io.File(directory).listFiles.filter(_.getName.endsWith(".nglog")) 
      // last modified returns to the millisecond date +%s returns to the second
      files.filter(_.lastModified/1000 > modTime.toLong).mkString(",") 
    }
}
