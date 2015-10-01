package files
import java.io.File
import java.security.MessageDigest

class modFiles {

    /* Recursive list of files http://stackoverflow.com/questions/2637643/how-do-i-list-all-files-in-a-subdirectory-in-scala */
    def recursiveListFiles(f: File): Array[File] = {
      val these = f.listFiles
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }

    /**
     * Reads directory recursively returing all files or files
     * with mod dates after modTime.
     */
    def logFiles(directory: String, modTime: String = "0"): String = {
      recursiveListFiles(new java.io.File(directory))
        .filter(_.getName.endsWith(".nglog")) 
      // last modified returns to the millisecond date +%s returns to the second
        .filter(_.lastModified/1000 > modTime.toLong)
        .map(_.getAbsolutePath)
        // create "filename, filename" string for spark ingest
        .mkString(",")
    }

    def md5(file: String) = {
      MessageDigest.getInstance("MD5").digest(file.getBytes).toString
    }
}
