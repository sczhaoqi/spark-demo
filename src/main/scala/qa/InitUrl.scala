package qa

import java.io.{File, PrintWriter}

import utils.StringTool._

import scala.util.Random

object InitUrl {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("E:/url.txt"),"UTF-8")
    for(i<-1 to 100000) {
      writer.write(this.virtualUrl+" "+this.virtualUrl+"\r\n")
    }
    writer.close()
  }
  def virtualUrl:String={
    val baseUrl=List("www.baidu.com","www.sf.com","www.google.com","spark.apache.org","www.qq.com","www.sina.com.cn")
    val length=Random.nextInt(10)
    val nn=Random.nextInt(100)%baseUrl.size

    baseUrl(nn)+"/"+RandomString(length,true)
  }
}

