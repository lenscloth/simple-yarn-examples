package org.lenscloth.hadoop.yarn.examples.functions

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration

object flatter {
  def main(args: Array[String]): Unit = {
    val conf = new YarnConfiguration()

    val inPath = new Path(args.head)
    val outPath = new Path(s"${args.head}.flatten")

    val hdfs = FileSystem.get(conf)

    val in = hdfs.open(inPath)
    val out = hdfs.create(outPath)

    val reader = new BufferedReader(new InputStreamReader(in))
    var line: String = reader.readLine()
    while (line!= null){
      out.writeUTF(line)
      out.writeUTF(".")
      line = reader.readLine()
    }

    out.close()
    in.close()
  }
}
