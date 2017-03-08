import org.apache.hadoop.fs.{FileSystem, Path}

object PathUtils {
  def toHDFSHome(fs: FileSystem, fileName: String) =
    new Path(fs.getHomeDirectory, fileName)
}
