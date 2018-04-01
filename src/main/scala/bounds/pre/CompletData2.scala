package bounds.pre

import java.io.{File, PrintWriter}

import scala.io.Source

object CompletData2 {

  def completData(line: String): String = {
    val data = line.split(";")
    val pcdData = data.apply(0).split("-")
    val lnts = data.apply(1)
    val lats = data.apply(2)

    val name = pcdData.apply(0)
    val code = pcdData.apply(1)
    val lnt: Array[Double] = lnts.split(",").map(x => x.toDouble)
    val lat: Array[Double] = lats.split(",").map(x => x.toDouble)
    var completLineStr = data.apply(0)
    if (lnt.length <= 1) {
      completLineStr += ";" + lnts.toDouble * 1000.toLong + "," + lats.toDouble * 1000.toLong
    } else {
      for (i <- 0 until lat.length - 1) {
        completLineStr += ";" + lnt(i) * 1000.toLong + "," + lat(i) * 1000.toLong
        val dx = lnt.apply(i + 1) * 1000 - lnt.apply(i) * 1000
        val dy = lat.apply(i + 1) * 1000 - lat.apply(i) * 1000
        var x = lnt.apply(i) * 1000
        var y = lat.apply(i) * 1000
        var step = 1
        if (Math.abs(dx) > Math.abs(dy))
          step = Math.abs(dx).toInt
        else
          step = Math.abs(dy).toInt
        val incrementx = dx / step;
        val incrementy = dy / step;
        for (nn <- 0 until step) {
          x += incrementx.toInt
          y += incrementy.toInt
          completLineStr + ";" + x + "," + y
        }
        completLineStr += ";" + lnt(i + 1) * 1000.toLong + "," + lat(i + 1) * 1000.toLong
      }
    }
    completLineStr
  }

  def main(args: Array[String]): Unit = {
    val completpath = "D:/phpStudy/WWW/Bounds/completData.txt"
    val filepath = "D:/phpStudy/WWW/Bounds/bounds.txt"
    val source = Source.fromFile(new File(filepath), "UTF-8")
    val writer = new PrintWriter(new File(completpath), "UTF-8")
    val lineIterator = source.getLines()

    for (line <- lineIterator) {
      writer.write(completData(line) + "\r\n")
    }
    writer.close()
    //    println(completData("圣方济各堂区-820008;113.602,113.604,113.604,113.598,113.580,113.579,113.576,113.561,113.560,113.554,113.554,113.553,113.553,113.554,113.564,113.564,113.565,113.567,113.568,113.568,113.568,113.569,113.571,113.572,113.573,113.574,113.575,113.576,113.577,113.578,113.579,113.581,113.582,113.583,113.584,113.589;22.138,22.133,22.132,22.125,22.110,22.109,22.108,22.106,22.106,22.107,22.111,22.117,22.121,22.125,22.127,22.128,22.128,22.128,22.128,22.129,22.132,22.132,22.132,22.132,22.132,22.133,22.133,22.134,22.135,22.135,22.135,22.135,22.136,22.136,22.138,22.144"))

  }
}

