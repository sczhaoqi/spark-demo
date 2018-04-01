package bounds.pre

import java.io.{File, PrintWriter}

import scala.io.Source

object SortedData {
  def main(args: Array[String]): Unit = {

    val sortedpath = "D:/phpStudy/WWW/Bounds/sortedData.txt"
    val filepath = "D:/phpStudy/WWW/Bounds/finalData.txt"
    val writer=new PrintWriter(new File(sortedpath))
    val source = Source.fromFile(new File(filepath), "UTF-8")
    val lineIterator = source.getLines()
    for (line <- lineIterator) {
      val data = line.split(";")
      val longitudeArray = new Array[Long](data.length - 1)
      for (i <- 1 until data.length) {
        longitudeArray(i - 1) = data.apply(i).split(":").apply(1).toDouble.toLong
      }
      writer.write(data.apply(0)+";"+longitudeArray.distinct.sorted.mkString(";")+"\r\n")
    }
    writer.close()
  }
}

