package files
import java.io.File
import java.security.MessageDigest

class modFiles {
    def dirAfter(directory: String, modTime: String): Array[String] = {
      // get list of all log files
      val files = new java.io.File(directory).listFiles.filter(_.getName.endsWith(".nglog")) 
      // last modified returns to the millisecond date +%s returns to the second
      files.filter(_.lastModified/1000 > modTime.toLong).map(_.getAbsolutePath) 
    }

    def md5(file: String) = {
      MessageDigest.getInstance("MD5").digest(file.getBytes).toString
    }
}
