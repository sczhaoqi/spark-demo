package qa

import java.io.{File, PrintWriter}

import scala.util.Random

object InitText2 {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("E:/word2.txt"),"UTF-8")
    for(i<-1 to 100) {
      val key=Random.nextInt(10)
      val value=Random.nextInt(10)
      writer.write(key+" "+value+"\r\n")
    }
    writer.close()
  }
}

