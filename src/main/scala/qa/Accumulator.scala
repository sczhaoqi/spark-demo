package qa

import java.io.File

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import utils.FileTool.deleteDir

object Accumulator {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf = new SparkConf().setAppName("SecondContacts").setMaster("local")
    val sc = new SparkContext(conf)

    val file = sc.textFile("E:/word.txt")
    //定义/注册累加器 ,也可以实现accumulator的接口
    val accumulator=new LongAccumulator
    accumulator.add(20)
    sc.register(accumulator)// 创建Accumulator[Int]并初始化为0
    val callSigns = file.flatMap(line => {
      if (line == "") {
        accumulator.add(1)// 累加器加1
      }
      line.split(" ")
    })
    val saveFloder=new File("E:/output.txt")
    deleteDir(saveFloder)
    callSigns.saveAsTextFile("E:/output.txt")
    println("Blank lines: " + accumulator.value)
  }
}
