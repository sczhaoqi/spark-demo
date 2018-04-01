package bounds.pre

import java.io.{File, PrintWriter}

import scala.collection.mutable
import scala.io.Source
//获取psd编码文件
object PSDCode {
  def main(args: Array[String]): Unit = {
    val pSDCodepath="D:/phpStudy/WWW/psdcode.csv"
    val codePath="D:/phpStudy/WWW/code.csv"
    val directDistrictCountryPath="D:/phpStudy/WWW/directDistrictCountry.csv" //直辖县文�?
    val pMap=new mutable.HashMap[String,String]()
    val sMap=new mutable.HashMap[String,String]()
    val source=Source.fromFile(new File(codePath),"UTF-8")
    val writer=new PrintWriter(pSDCodepath,"UTF-8")
    val dwriter=new PrintWriter(directDistrictCountryPath,"UTF-8")
    val lineIterator=source.getLines()
    //读取省市
    for(line<- lineIterator){
      val data=line.split(",")
      val code=data.apply(1)
      val name=data.apply(0)
      if(code.endsWith("0000"))
        pMap.put(code.substring(0,2),name)
      else if(code.endsWith("00")){
        sMap.put(code.substring(0,4),name)
      }
    }
    val source2=Source.fromFile(new File(codePath),"UTF-8")
    val lineIterator2=source2.getLines()
    for(line<- lineIterator2){
      val data=line.split(",")
      val code=data.apply(1)
      val name=data.apply(0)
      if(code.endsWith("0000")) {
        writer.write(code+","+name+", , "+"\r\n")
      }
      else if(code.endsWith("00")){
        writer.write(code+","+pMap.get(code.substring(0,2)).getOrElse(" ")+","+name+", "+"\r\n")
      }
      else
      {
        if(!name.equals("")&& sMap.get(code.substring(0,4)).getOrElse("").equals(""))
          dwriter.write(code+","+pMap.get(code.substring(0,2)).getOrElse("")+","+sMap.get(code.substring(0,4)).getOrElse("")+","+name+"\r\n")
        writer.write(code+","+pMap.get(code.substring(0,2)).getOrElse(" ")+","+sMap.get(code.substring(0,4)).getOrElse(" ")+","+name+"\r\n")
      }
    }
    writer.close()
    dwriter.close()
  }
}

