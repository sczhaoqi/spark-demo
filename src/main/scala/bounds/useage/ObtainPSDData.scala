package bounds.useage

import java.io.File

import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source



object ObtainPSDData {
  private val logger = Logger.getLogger(ObtainPSDData.getClass)
  //二维数组 [纬度][经度]
  val boundsArray = new ArrayBuffer[Array[Long]]()
  //二维数组 对应的code
  val boundsNameArray = new ArrayBuffer[Array[String]]()

  val pSDCodeMap = new mutable.HashMap[String, String]()
  var maxLatinFile = 1L
  var minLatinFile = 1L

  def otainPCDCode(lnt: Long, lat: Long): String = {

    if (lat >= maxLatinFile || lat <= minLatinFile) {
      "UnKnown"
    } else {
      binarySearch(lat, lnt)
    }
  }

  /** *
    *
    * @param latit  当前纬度纬度
    * @param longit 当前查询地区的经度
    * @return 地区名称(省市区)
    */
  def binarySearch(latit: Long, longit: Long): String = {
    //所给的经度不在对应的边界值的最大/最小范围内
    if (maxLatinFile < latit || minLatinFile > latit) {
      return "UnKnown"
    } else {
      val bounsArrNum = latit - minLatinFile
      //补全的数据,则其对应的下标值等于其纬度-最小的纬度
      if (bounsArrNum == -1)
        return "UnKnown"
      val bounsArr = boundsArray.apply(bounsArrNum.toInt)
      //查找经度
      var low = 0
      var high = bounsArr.length - 1
      var middle=high
      while (low < high ) {
        if(bounsArr(low)>longit) {
          return "UnKnown"
        }
        if(bounsArr(high)<longit){
          return boundsNameArray(bounsArrNum.toInt).apply(high)
        }
        middle = (low + high) / 2
        if(middle==low){
          return boundsNameArray(bounsArrNum.toInt).apply(low)
        }
        val A = bounsArr(middle)
        if(longit==A) {
          return boundsNameArray(bounsArrNum.toInt).apply(middle)
        }
        if (longit < A) {
          high = middle
        }
        else{
          low = middle
        }
      }
      return boundsNameArray(bounsArrNum.toInt).apply(low)
    }
  }

  /** *
    * 读取文件并初始化相关变量的值
    */
  def init() = {
    /** *
      * BoundsCodeData为处理完成的边界值的数据文件
      * 每行由一个Latitude纬度 多个经度-右侧所属区域编码构成 以;分割
      * 行与行之间存在连续性
      */
    val savepath = "D:/phpStudy/WWW/Bounds/BoundsCodeData.txt"
    val sortedData = Source.fromFile(new File(savepath), "UTF-8")
    /** *
      * psdcode为处理完成的编码对应表的文件
      * 每行为一个地区编码数据
      * 包含 code,省,市,区 以"," 分割
      */
    val psdCodepath = "D:/phpStudy/WWW/Bounds/psdcode.csv"
    val psdCode = Source.fromFile(new File(psdCodepath), "UTF-8")

    val sortedDataLineIterator = sortedData.getLines()

    //读取边界数据至内存
    for (line <- sortedDataLineIterator) {
      val latStr = line.split(";").apply(0)
      if (minLatinFile == 1.0) {
        minLatinFile = latStr.toLong
      }
      maxLatinFile = latStr.toLong
      val boundsArr = line.split(";").filter(x => !x.equals(latStr)).map(x => x.split("-").apply(0).toLong)
      val boundsNameArr = line.split(";").filter(x => !x.equals(latStr)).map(x => x.split("-").apply(1))
      boundsArray += boundsArr
      boundsNameArray += boundsNameArr
    }

    //读取编码表至内存
    val psdCodeLineIterator = psdCode.getLines()
    for (line <- psdCodeLineIterator) {
      if (!line.trim.equals("")) {
        val data = line.split(",")
        var code = ""
        var province = ""
        var city = ""
        var distinct = ""
        if (data.length > 0) {
          code = data.apply(0)
          if (data.length > 1)
            province = data.apply(1)
          if (data.length > 2)
            city = data.apply(2)
          if (data.length > 3)
            distinct = data.apply(3)
          pSDCodeMap.put(code, province + city + distinct)
        }
      }
    }

  }

  //读取文件,文件的每行数据格式为 Lat;经度-地区
  //其中 纬度值 并不连续,作补齐操作
  def main(args: Array[String]): Unit = {

    init();

    println(pSDCodeMap.get(otainPCDCode( 113901,22542)).getOrElse("UnKnown"))
    //    val startTime = System.nanoTime()
    //    val slongitude = 110000
    //    val elongitude = 120000
    //    val slatitude = 30000
    //    val elatitude = 35000
    //    val times = (elatitude - slatitude) * (elongitude - slongitude);
    //    for (i <- slongitude to elongitude) {
    //      for (j <- slatitude to elatitude) {
    //        val ccc = otainPCDCode(i, j)
    //        println(i + "," + j + ":" + ccc + ":" + pSDCodeMap.get(ccc).getOrElse("UnKnown"))
    //      }
    //    }
    //
    //    val endTime = System.nanoTime()
    //    println("all time spend as run " + times + "times:" + (endTime - startTime) + "ms")
  }
}