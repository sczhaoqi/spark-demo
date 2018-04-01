package bounds.pre

import java.io.{File, PrintWriter}

import scala.io.Source

object CompletBoundsCodeData {
  def main(args: Array[String]): Unit = {
    val source=Source.fromFile(new File("D:\\phpStudy\\WWW\\bounds\\saveCodeData.txt"),"UTF-8")
    val savepath="D:\\phpStudy\\WWW\\bounds\\BoundsCodeData.txt"
    val writer=new PrintWriter(new File(savepath),"utf-8")
    val lineIterator=source.getLines()
    var beforlat=0L
    for(line<- lineIterator){
      val code =line.split(";").apply(0).toLong
      if(beforlat==0)
        beforlat=code-1
      if(code==beforlat+1){
        beforlat=code
        writer.write(line+"\r\n")
      }else{
        for(i<-beforlat until code){
          writer.write(i+";"+"\r\n")
        }
        beforlat=code
      }
    }
    writer.close()
  }
}

