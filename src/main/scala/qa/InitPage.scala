package qa

import java.io.{File, PrintWriter}

import scala.util.Random

object InitPage {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("E:/page.txt"),"UTF-8")
    for(i<-1 to 100000000) {
      val a='a'
      val A='A'
      val aaa=Random.nextInt(26)
      val bbb=Random.nextInt(26)
      var aa='a';
      var bb='a'
      if(Random.nextInt(2)==1)
        aa=A
      if(Random.nextInt(2)==1)
        bb=A
      writer.write((aa+aaa).toChar+" "+(aa+bbb).toChar+"\r\n")
    }
    writer.close()
  }
}

