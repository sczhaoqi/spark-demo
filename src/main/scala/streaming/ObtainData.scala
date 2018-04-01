package streaming

import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter}
import javafx.print.Printer

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
// 导入以下包
import scala.util.control._
object ObtainData {

  object PointData {
    val minLat =3.0
    val maxLat =54.0
    val minLnt=73.0
    val maxLnt=135.0

    val countryboundsLnt=new ArrayBuffer[Array[Double]]()
    val countryboundsLat=new ArrayBuffer[Array[Double]]()

    val countryCode="1"
    val provinceName=new ArrayBuffer[String]()
    val provinccodes=new ArrayBuffer[String]()///编码
    val provinceboundsLat=new ArrayBuffer[Array[Double]]()
    val provinceboundsLnt=new ArrayBuffer[Array[Double]]()

    val citycodes=new ArrayBuffer[String]()
    val cityName=new ArrayBuffer[String]()
    val preProvinceCode=new ArrayBuffer[String]()//保存前两位,上级省的编码
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
      var prpcode="" //所属省份代码
      var prccode="" //所属市区代码
      var rcode=""  //自身代码

      if(longitude>maxLnt||longitude<minLnt||latitude>maxLat||latitude<minLat)//粗略判断是否在中国
        return resultStr
      // 创建 Breaks 对象
      //    val countryloop = new Breaks;
      //
      //    // 在 breakable 中循环
      //    countryloop.breakable {
      //      for (i <- 0 until countryboundsLat.length) //细致判断是否在中国
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
            prpcode=provinccodes(i).substring(0,2) //记录当前的省区
            resultStr=provinceName.apply(i)//保存当前的省名称
            provinceloop.break()
          }
        }
        return "外国"
      }

      val cityloop = new Breaks;
      cityloop.breakable{
        for(i<-0 until cityboundsLnt.length){
          if(prpcode.equals(preProvinceCode.apply(i))){//此地址可能位于当前市区,当前省
            if(pointInPolygon(longitude,latitude,cityboundsLnt(i),cityboundsLat(i))){
              prccode=citycodes(i).substring(0,4)
              resultStr+=cityName.apply(i)
              cityloop.break()
            }
          }
        }
        return resultStr
      }
      val distinctloop=new Breaks;
      distinctloop.breakable{
        for(i<-0 until distinctboundsLat.length){
          if(prccode.equals(preCityCode.apply(i))){//此地址可能位于当前地区,当前市
            if(pointInPolygon(longitude,latitude,distinctboundsLnt(i),distinctboundsLat(i))) {
              resultStr+=distinctName.apply(i)
              distinctloop.break()
            }
          }
        }
        return resultStr
      }
      resultStr
    }
    def main(args: Array[String]): Unit = {
      val filepath="/zhaoqi/bounds.txt"
      val slongitude=73000
      val elongitude=135000
      val slatitude=3000
      val elatitude=60000
      val conf = new SparkConf().setAppName("ObaData")
      val spark=new SparkContext(conf)

      val source = Source.fromFile(new File(filepath), "UTF-8")
      spark.textFile(filepath).foreach(
        line=>{
          val data=line.split(";")
          val pcdData=data.apply(0).split("-")
          val lnts=data.apply(1)
          val lats=data.apply(2)

          val name=pcdData.apply(0)
          val code=pcdData.apply(1)
          val lnt:Array[Double]=lnts.split(",").map(x=>x.toDouble)
          val lat:Array[Double]=lats.split(",").map(x=>x.toDouble)
          if(code.endsWith("00000"))//国家:中国
          {
            countryboundsLnt+=lnt
            countryboundsLat+=lat
          }else if(code.endsWith("0000")){//省
            provinceboundsLat+=lat
            provinceboundsLnt+=lnt
            provinccodes+=code
            provinceName+=name
          }else if(code.endsWith("00")){//市
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
      )
      for (i<-slongitude to elongitude) {
        val rdd = spark.parallelize(slatitude to elatitude,100).map(j=>{
          val psdStr = otainPCDData(i.toDouble / 1000.0, j.toDouble / 1000.0)
          if (!psdStr.equals("外国"))
            i.toDouble / 1000.0 + "," + j.toDouble / 1000.0 + ":" + psdStr + ";"
          else
            ""
        })
        rdd.saveAsTextFile("/zhaoqi/grid/data" + i)
      }
    }
  }
}