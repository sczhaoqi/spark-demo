package hadoop

import java.io.{File, PrintWriter}

import scala.util.Random

object InitNum {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("E:/numbers.txt"),"UTF-8")
    for(i<-1 to 100000000) {
      val str=Random.nextInt(1000000)
      writer.write(str+"\r\n")
    }
    writer.close()
  }
}

