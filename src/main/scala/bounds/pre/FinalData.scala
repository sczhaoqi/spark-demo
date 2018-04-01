package bounds.pre

import java.io.{File, PrintWriter}

import scala.io.Source
object FinalData{
  def main(args: Array[String]): Unit = {
    val finalpath = "D:/phpStudy/WWW/Bounds/finalData.txt"
    val filepath = "D:/phpStudy/WWW/Bounds/completData.txt"
    val source = Source.fromFile(new File(filepath), "UTF-8")
    val writer=new PrintWriter(new File(finalpath))
    val lineIterator = source.getLines()
    val slatitude = 3500
    val elatitude = 54000
    var locationArray=new Array[String](elatitude-slatitude)
    for (line <- lineIterator) {
      val data = line.split(";")
      val pcdData = data.apply(0)
      val latnn=data.apply(1)
      for(i<-1 until data.length){
        val lntlat=data.apply(i)
        val lat=lntlat.split(",").apply(1).toFloat.toInt
//        println(lat)
//        if(!pcdData.split("-").apply(1).endsWith("00"))
          locationArray(lat-slatitude)+=pcdData+":"+lntlat.split(",").apply(0)+";"
      }
    }
    for(i<-0 until  locationArray.length){
      if(locationArray(i)!=null){
        writer.write((i+slatitude)+";"+locationArray(i).replace("null","")+"\r\n")
      }
    }
    writer.close()
  }
}

