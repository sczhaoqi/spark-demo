package bounds.pre

import java.io.{File, PrintWriter}
import java.util

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
// 导入以下�?
import scala.util.control._
//过程中部分文件中的经纬度均扩大1000倍,保存为整数形式
// Bounds 第一步 爬取边界值,保存文件 格式为citycode;经度,纬度,经度,纬度
// TBounds 下一步 处理数据边界将精度降低至0.001
// CompletData2 下一步 补全边界值completData2 按照计算机图形学的方式 描点划线
// FinalData 下一步 过滤省份数据,并将边界值按照纬度顺序,一行一行的写入文件,每一行数据格式为 纬度;psdData:经度;下一个psdData:经度...
// SortedData 下一步 将每行数据按照经度从小到大排序,并过滤重复的值,重新写入文件,每行数据格式为 纬度;经度;经度...
// PointDataCode 下一步 取每行数据,将经度右移0.5个单位(即经度+0.0005)获得每个边界点右侧的地区psdCode值 最终的边界数据文件
object PointDataCode {
  val minLat =3.0
  val maxLat =54.0
  val minLnt=73.0
  val maxLnt=135.0
  val directDistrictCountry=new util.ArrayList[String]() //保存直辖�?

  val countryboundsLnt=new ArrayBuffer[Array[Double]]()
  val countryboundsLat=new ArrayBuffer[Array[Double]]()

  val countryCode="1"
  val provinceName=new ArrayBuffer[String]()
  val provinccodes=new ArrayBuffer[String]()///编码
  val provinceboundsLat=new ArrayBuffer[Array[Double]]()
  val provinceboundsLnt=new ArrayBuffer[Array[Double]]()

  val citycodes=new ArrayBuffer[String]()
  val cityName=new ArrayBuffer[String]()
  val preProvinceCode=new ArrayBuffer[String]()//保存前两�?,上级省的编码
  val cityboundsLat=new ArrayBuffer[Array[Double]]()
  val cityboundsLnt=new ArrayBuffer[Array[Double]]()

  val distinctcodes=new ArrayBuffer[String]()//编码
  val preCityCode=new ArrayBuffer[String]() //保存前四位上级市区的编码
  val distinctName=new ArrayBuffer[String]()
  val distinctboundsLat=new ArrayBuffer[Array[Double]]()
  val distinctboundsLnt=new ArrayBuffer[Array[Double]]()

  def pointInPolygon(longitude:Double,latitude:Double,lntsArr:Array[Double],latsArr:Array[Double]):Boolean= {
    var oddNodes = false
    var j= lntsArr.length-1
    for (i<-0 until lntsArr.length)
    {
      if ((latsArr.apply(i) < latitude && latsArr.apply(j)>= latitude
        || latsArr.apply(j) < latitude && latsArr.apply(i) >= latitude)
        && (lntsArr.apply(i) <= longitude || lntsArr.apply(j) <= longitude)) {
        if(lntsArr.apply(i)+(latitude-latsArr.apply(i))/(latsArr.apply(j)-latsArr.apply(i))*(lntsArr.apply(j)-lntsArr.apply(i))<longitude) {
          oddNodes= !oddNodes;
        }
      }
      j = i;
    }
    return oddNodes;
  }
  def otainPCDData(longitude:Double,latitude:Double): String ={
    var resultStr="外国"
    var prpcode="" // 属省份代
    var prccode="" // 属市区代
    var rcode="900000"  //自身代码

    if(longitude>maxLnt||longitude<minLnt||latitude>maxLat||latitude<minLat)//粗略判断是否在中
      return resultStr+"-"+rcode
    // 创建 Breaks 对象
//    val countryloop = new Breaks;
//
//    // breakable 中循
//    countryloop.breakable {
//      for (i <- 0 until countryboundsLat.length) //细致判断是否在中
//      {
//        if(pointInPolygon(longitude,latitude,countryboundsLnt(i),countryboundsLat(i))){
//          countryloop.break()
//        }
//      }
//      return resultStr
//    }

    val provinceloop = new Breaks;
    provinceloop.breakable{
      for(i <-0 until provinceboundsLat.length){
        if(pointInPolygon(longitude,latitude,provinceboundsLnt(i),provinceboundsLat(i))){
          prpcode=provinccodes(i).substring(0,2) //记录当前的省
          rcode=provinccodes(i)
          resultStr=provinceName.apply(i)//保存当前的省名称
          provinceloop.break()
        }
      }
      return "外国"+"-"+rcode
    }

    val cityloop = new Breaks;
    cityloop.breakable{
      for(i<-0 until cityboundsLnt.length){
        if(prpcode.equals(preProvinceCode.apply(i))){//此地可能位于当前市区,当前
          if(pointInPolygon(longitude,latitude,cityboundsLnt(i),cityboundsLat(i))){
            prccode=citycodes(i).substring(0,4)
            rcode=citycodes(i)
            resultStr+=cityName.apply(i)
            cityloop.break()
          }
        }
      }
      return resultStr+"-"+rcode
    }
    val distinctloop=new Breaks;
    distinctloop.breakable{
      for(i<-0 until distinctboundsLat.length){
        if(prccode.equals(preCityCode.apply(i))){//此地可能位于当前地区,当前
          if(pointInPolygon(longitude,latitude,distinctboundsLnt(i),distinctboundsLat(i))) {
            resultStr+=distinctName.apply(i)
            rcode=distinctcodes(i)
            distinctloop.break()
          }
        }
      }
      return resultStr+"-"+rcode
    }
    resultStr+"-"+rcode
  }
  def main(args: Array[String]): Unit = {
    val filepath="D:/phpStudy/WWW/Bounds/bounds.txt" //边界区域
    val directDistrictCountryPath="D:/phpStudy/WWW/Bounds/directDistrictCountry.csv" //直辖
    val source = Source.fromFile(new File(filepath), "UTF-8")
    val dsource = Source.fromFile(new File(directDistrictCountryPath), "UTF-8")
    //首先读取直辖县文�?,将直辖县加入列表�?
    val dlineIterator=dsource.getLines()
    for(line<-dlineIterator){
      if(!line.trim.equals("")){
        val dcode=line.split(",").apply(0)
        directDistrictCountry.add(dcode)
      }
    }
    val lineIterator=source.getLines()

    for (line <- lineIterator){
      val data=line.split(";")
      val pcdData=data.apply(0).split("-")
      val lnts=data.apply(1)
      val lats=data.apply(2)

      val name=pcdData.apply(0)
      val code=pcdData.apply(1)
      val lnt:Array[Double]=lnts.split(",").map(x=>x.toDouble)
      val lat:Array[Double]=lats.split(",").map(x=>x.toDouble)
      if(code.equals("100000"))//国家:中国
      {
        countryboundsLnt+=lnt
        countryboundsLat+=lat
      }else if(code.endsWith("0000")){//�
        provinceboundsLat+=lat
        provinceboundsLnt+=lnt
        provinccodes+=code
        provinceName+=name
      }else if(code.endsWith("00")){//
        cityboundsLat+=lat
        cityboundsLnt+=lnt
        citycodes+=code
        preProvinceCode+=code.substring(0,2)
        cityName+=name
      }else if(directDistrictCountry.contains(code)){//直辖 直辖县当做市处理
        cityboundsLat+=lat
        cityboundsLnt+=lnt
        citycodes+=code
        preProvinceCode+=code.substring(0,2)
        cityName+=name
      }else{//地区
        distinctboundsLat+=lat
        distinctboundsLnt+=lnt
        distinctName+=name
        distinctcodes+=code
        preCityCode+=code.substring(0,4)
      }
    }
    println("all in memory")



//    val times= 100000;
//    val startTime=System.nanoTime()
//    val slongitude=73000
//    val elongitude=135000
//    val slatitude=3000
//    val elatitude=60000
//    for(i<-slongitude to elongitude){
//      println(i)
//      val writer=new PrintWriter(new File("D:/phpStudy/WWW/Bounds/resData"+i+".txt"),"UTF-8")
//      for(j<-slatitude to elatitude) {
//        val psdStr = otainPCDData(i.toDouble / 1000.0, j.toDouble / 1000.0)
//        if (!psdStr.equals("外国"))
//          writer.write(i.toDouble / 1000.0 + "," + j.toDouble / 1000.0 + ":" + psdStr + ";")
//      }
//      writer.close()
//    }
//    val endTime=System.nanoTime()
//    println("all time spend as run "+times+"times:"+(endTime-startTime)+"ms")

    val sortedpath = "D:/phpStudy/WWW/Bounds/sortedData.txt"
    val savepath = "D:/phpStudy/WWW/Bounds/saveCodeData.txt"
    val sortedData=Source.fromFile(new File(sortedpath),"UTF-8")
    val savewriter=new PrintWriter(new File(savepath),"UTF-8")
    val sortedDatalineIterator=sortedData.getLines()
    for (line<- sortedDatalineIterator){
      val data=line.split(";")
      savewriter.write(data.apply(0))
      val lat=data.apply(0).toDouble/1000
      for(i<- 1 until data.length){
        savewriter.write(";"+data.apply(i)+"-"+otainPCDData((data.apply(i).toDouble+0.5)/1000,lat).split("-").apply(1))
      }
      savewriter.write("\r\n")
    }
    savewriter.close()
  }
}

