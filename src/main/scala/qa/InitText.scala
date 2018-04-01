package qa

import java.io.{File, PrintWriter}

import scala.util.Random

object InitText {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("E:/word.txt"),"UTF-8")
    for(i<-1 to 10000000) {
      val str=Random.nextInt(1000)
      writer.write(str+" ")
    }
    writer.close()
  }
}

