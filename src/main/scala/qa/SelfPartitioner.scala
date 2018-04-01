package qa

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object SelfPartitioner {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf = new SparkConf().setAppName("SecondContacts").setMaster("local")
    val sc = new SparkContext(conf)
    val List_Friends = sc.textFile("E:/url.txt").cache()
  }

  class DomainNamePartitioner(numParts: Int) extends Partitioner {
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int = {
      val domain = new URL(key.toString).getHost()
      val code = (domain.hashCode % numPartitions)
      if(code < 0) {
        code + numPartitions // 使其非负
      }else{
        code
      }
    }
    // 用来让Spark区分分区函数对象的Java equals方法
    override def equals(other: Any): Boolean = other match {
      case dnp: DomainNamePartitioner =>
        dnp.numPartitions == numPartitions
      case _ =>
        false
    }
  }
}

