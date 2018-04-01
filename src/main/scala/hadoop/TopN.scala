package hadoop

import java.io._

import org.apache.log4j.Logger
import org.apache.log4j.spi.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

object TopN {
  private val loggger = Logger.getLogger(TopN.getClass)
  def getPartition(key: Int): Int = {
    0
  }

  /***
    * @param partitionNums 分区数
    * @param file 输入文件地址
    * @return 数据总数 样本数组
    */
  def sampleData(partitionNums:Int,sizeRate:Int,file:String):(Long,Array[Long])={
    val source = Source.fromFile(new File(file), "UTF-8")
    // 第一个参数可以是字符串或java.io.File
    val lineIterator = source.getLines
    var nums = 0 //总数
    var lag = 0 //当前数目
    val sampleSize=math.min(partitionNums*sizeRate,1e6) //最大采样数为1e6,采样为分区的20倍
    var sampleArray = new Array[Long](sampleSize.toInt) //保存取样数据

    //扫描第一遍
    for (num <- lineIterator) {
      lag += 1 //当前数目+1
      if (lag < sampleSize){
        sampleArray(lag) = num.toLong //抽取前n个
      }
      else {
        val tmpRate = Random.nextInt(lag + 1) //当前数目为lag
        if (tmpRate < sampleSize)
          sampleArray(tmpRate) = num.toLong //随机 (0,lag-1),当随机产生的数小于需抽取的数目替换对应位置已经保存的数
      }
    }
    source.close()
    (lag,sampleArray)
  }

  def sortedFile(stagesPath: String) = {
    val source = Source.fromFile(new File(stagesPath), "UTF-8")
    val lineIterator = source.getLines
    val array=new ArrayBuffer[Long]()
    for (num <- lineIterator) {
      array+=num.toLong;
    }

    val writer =new BufferedWriter(new OutputStreamWriter(new FileOutputStream(stagesPath, false)))
    writer.write(array.sorted.toString())
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    //读取,分区,排序,合并
    val partitionNums=100
    val sizeRate=20
    val filepath="E:/numbers.txt"
    loggger.info("开始抽样")
    //采用水塘抽样 扫描全部文件
    val reservoirSampleAndCount=sampleData(partitionNums,sizeRate,filepath)
    val sum=reservoirSampleAndCount._1 //原始数据总数
    val sampleArray=reservoirSampleAndCount._2 //抽样数组
    val sortedSampleArray=sampleArray.sorted //对数组排序
    val rangePartitioning=new Array[Long](partitionNums-1)
    //得到分区范围数组
    for(i<-1 until sortedSampleArray.size){
      if(i%sizeRate==0){
        rangePartitioning(i/sizeRate-1)=sortedSampleArray(i)
      }
    }
    rangePartitioning.foreach(x=>println(x))
    loggger.info("抽样完成,开始分区")
    //获得分区号,保存到指定分区中
    val source = Source.fromFile(new File(filepath), "UTF-8")
    // 第一个参数可以是字符串或java.io.File
    val lineIterator = source.getLines
    //遍历第二遍
    val writer=new PartitionWriterHolder()
    for (num <- lineIterator) {
      //      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("E:/num/numbers" + getRangePartitionNum(num.toLong,rangePartitioning).toString + ".txt", true)))
      //      writer.append(num+"\r\n")
      //      writer.close()
      writer.write(getRangePartitionNum(num.toLong,rangePartitioning),num)
    }
    //关闭文件写入
    writer.close()
    loggger.info("分区完成,开始排序")
    for(i<-0 to partitionNums-1){
      val stagesPath="E:/num/numbers"+i+".txt"
      sortedFile(stagesPath)
    }
    loggger.info("排序完成")
  }
  def getRangePartitionNum(num:Long,rangePartitioning:Array[Long]): Int ={
    val rangeSize=rangePartitioning.size;
    for(i<-0 to rangeSize-1)
    {
      if(i==0&&num<rangePartitioning(0))
        return 0
      if(num>=rangePartitioning(rangeSize-1))
        return rangeSize
      if(i!=0&&num>=rangePartitioning(i-1)&&num<rangePartitioning(i))
        return i
    }
    0
  }
}