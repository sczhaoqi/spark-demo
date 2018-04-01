package qa

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import utils.FileTool.deleteDir

object PageRank {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf= new SparkConf()
    //    conf.setMaster("local")
    conf.setMaster("local[2]")
    conf.setAppName("Test on local")
    val spark=new SparkContext(conf)

    val text=spark.textFile("E:/page.txt")
    //52个字符a-z A-Z的对
    // 将每个页面的排序值初始化为1.0；由于使用mapValues，生成的RDD
    // 的分区方式会和"links"的一样
    //使用groupby,partitionBy的方式可以指定分区数目
    //groupByKey(分区数).指定分区方式partitionBy(new HashPartitioner(10))/
    val links=text.map(line=>{
      val kv=line.split(" ")
      (kv(0),kv(1))
    }).groupByKey()

    var ranks = links.mapValues(v => 1.0)
    // 运行10轮PageRank迭代
    for(i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) =>
          links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
    }
    // 写出最终排名
    val saveFloder=new File("E:/ranks")
    deleteDir(saveFloder)
    ranks.saveAsTextFile("E:/ranks")
  }

}