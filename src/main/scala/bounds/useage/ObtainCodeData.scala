package bounds.useage

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.io.Source



object ObtainCodeData {
  //二维数组 [纬度][经度]
  val boundsArray = new ArrayBuffer[Array[Long]]()
  //二维数组 对应的code
  val boundsNameArray = new ArrayBuffer[Array[String]]()
  //一维数组 对应的纬度值
  val boundsLatArray = new ArrayBuffer[Long]()

  var maxLatinFile = 1L
  var minLatinFile = 1L

  def otainPCDData(lnt: Long, lat: Long): String = {

    if (lat >= maxLatinFile || lat <= minLatinFile) {
      "UnKnown"
    } else {
      binarySearch(lat, lnt)
    }
  }

  /***
    * @deprecated 不使用此方式获取数组下标,而是直接将原始数据补齐,通过Lat直接获得下标
    * @param latit 纬度值
    * @return
    */
  def getLatArrNum(latit: Long) :Int={
    var low=0
    var high=boundsLatArray.length-1
    while (low < high) {
      val middle = (low + high) / 2

      if (boundsLatArray(middle)==latit) {
        return middle
      }
      else if (latit < boundsLatArray(middle)) high = middle - 1
      else low = middle + 1
    }
    -1
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
      var middle=0
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

  //读取文件,文件的每行数据格式为 Lat;经度-地区
  //其中 纬度值 并不连续,需要作补齐操作comletcodeData
  def main(args: Array[String]): Unit = {
    val savepath = "D:/phpStudy/WWW/Bounds/saveCodeData.txt"
    val sortedData = Source.fromFile(new File(savepath), "UTF-8")
    val sortedDataLineIterator = sortedData.getLines()

    for (line <- sortedDataLineIterator) {
      val latStr = line.split(";").apply(0)
      if (minLatinFile == 1.0) {
        minLatinFile = latStr.toLong
      }
      maxLatinFile = latStr.toLong
      boundsLatArray += maxLatinFile
      val boundsArr = line.split(";").filter(x => !x.equals(latStr)).map(x => x.split("-").apply(0).toLong)
      val boundsNameArr = line.split(";").filter(x => !x.equals(latStr)).map(x => x.split("-").apply(1))
      boundsArray += boundsArr
      boundsNameArray += boundsNameArr
    }
    println("all in memory")


    val startTime = System.nanoTime()
    val slongitude = 73000
    val elongitude = 135000
    val slatitude = 3000
    val elatitude = 60000
    val times = (elatitude - slatitude) * (elongitude - slongitude);
    for (i <- slongitude to elongitude) {
      for (j <- slatitude to elatitude) {
        otainPCDData(i, j)
      }
    }
    val endTime = System.nanoTime()
    println("all time spend as run " + times + "times:" + (endTime - startTime) + "ms")
  }
}

