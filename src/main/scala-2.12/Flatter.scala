import java.io.{ File, PrintWriter }
import scala.io.Source

object Flatter {
  def main(args: Array[String]): Unit = {
    val filename = args.head
    val flatfilename = args(1)
    val pw = new PrintWriter(new File(s"./$flatfilename"))
    for (line <- Source.fromFile(filename).getLines()) {
      pw.write(line)
      pw.write(".")
    }
    pw.close()
  }
}
