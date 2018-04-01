package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamingKafKa {
  val brokers = "192.168.192.145:9092,192.168.192.148:9092,192.168.192.149:9092"
  val topics = "kafkaData"
  val checkpointPath="E:/kafkaData"
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")

  //1.对应的处理部分必须放在函数体内
  //2.checkpoint的元数据会记录jar的序列化的二进制文件，因为你改动过代码，然后重新编译，新的序列化jar文件，在checkpoint的记录中并不存在，所以就导致了上述错误，如何解决：
  //  也非常简单，删除checkpoint开头的的文件即可，不影响数据本身的checkpoint
  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("Kafka+Streaming").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(10))   // new context
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val lines = KafkaUtils.createDirectStream[String, String,StringDecoder, StringDecoder](ssc,kafkaParams,topicSet)
    //val message = lines.map(_._1) map(_._1)  数据是空的 null
    val message = lines.map(_._2) //map(_._2)  才是Kafka里面打入的数据
    val words = message.flatMap(_.split(" "))

    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    //message.print()  checked
    ssc.checkpoint(checkpointPath)   // set checkpoint directory
    ssc
  }
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointPath,functionToCreateContext _)//每10s处理一次
    ssc.start()
    ssc.awaitTermination()
  }

}

