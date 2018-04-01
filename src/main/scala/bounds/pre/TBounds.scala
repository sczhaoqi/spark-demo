package bounds.pre

import java.io.{File, PrintWriter}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
//过程中部分文件中的经纬度均扩大1000倍,保存为整数形式
// Bounds 第一步 爬取边界值,保存文件 格式为citycode;经度,纬度,经度,纬度
// TBounds 下一步 处理数据边界将精度降低至0.001
// CompletData2 下一步 补全边界值completData2 按照计算机图形学的方式 描点划线
// FinalData 下一步 过滤省份数据,并将边界值按照纬度顺序,一行一行的写入文件,每一行数据格式为 纬度;psdData:经度;下一个psdData:经度...
// SortedData 下一步 将每行数据按照经度从小到大排序,并过滤重复的值,重新写入文件,每行数据格式为 纬度;经度;经度...
// PointDataCode 下一步 取每行数据,将经度右移0.5个单位(即经度+0.0005)获得每个边界点右侧的地区psdCode值 最终的边界数据文件
object TBounds {
  def subdirs(dir: File): Iterator[File] = {
    val f = dir.listFiles.toIterator
    f
  }
  def main(args: Array[String]): Unit = {
    val filepath="D:/phpStudy/WWW/bounds"
    val savepath="D:/phpStudy/WWW/Bounds/bounds.txt"
    val writer=new PrintWriter(savepath)
    subdirs(new File(filepath)).foreach(f=>{
      val source = Source.fromFile(f, "UTF-8")
      val linesIterator=source.getLines()
      val line=linesIterator.next()
      var city=""
      val citycode=f.getName.split("-").apply(0)
      println(citycode)
      if(line.contains(";")) {
        val linedata = line.split(";")
        city=linedata.apply(0)
        val bounds=linedata.apply(1).split(",")
        var lnt=0.0
        var lat=0.0
        val boundsArray=new ArrayBuffer[String]()
        for(i<-0 until bounds.length/2){
          lnt = bounds.apply(i*2).toDouble
          lat = bounds.apply(i*2+1).toDouble
          boundsArray+=f"$lnt%1.3f"+","+f"$lat%1.3f"
        }
        val lntlat=boundsArray.distinct
        val lntArray=new ArrayBuffer[String]()
        val latArray=new ArrayBuffer[String]()
        for(i<-0 until lntlat.length){
          lntArray+=lntlat.apply(i).split(",").apply(0)
          latArray+=lntlat.apply(i).split(",").apply(1)
        }
        writer.write(city+"-"+citycode+";"+lntArray.mkString(",")+";"+latArray.mkString(",")+"\r\n")
      }
    })
    writer.close()

  }
}

