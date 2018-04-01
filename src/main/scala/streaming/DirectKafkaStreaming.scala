package streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{Time, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.spark_project.guava.base.Stopwatch

object DirectKafkaStreaming {
  private val logger = Logger.getLogger(DirectKafkaStreaming.getClass)
  def main(args: Array[String]): Unit = {
    val brokers = "192.168.192.145:9092,192.168.192.148:9092,192.168.192.149:9092"
    //zk集群的地址
    val zkServers = "192.168.192.145:2181,192.168.192.148:2181,192.168.192.149:2181";
    val topics = "kafkaData"
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val checkpointDir = "E:/directStreaming"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicsSet = topics.split(",").toSet
    val conf = new SparkConf().setAppName("DirectKafkaStreaming").setMaster("local[2]")
    val sc = new SparkContext(conf) //create spark and memsql context
    val ssc =  setupSsc(topicsSet, kafkaParams, checkpointDir,sc,zkServers)
    /* Start the spark streaming   */
    ssc.start()
    ssc.awaitTermination();
  }
  def setupSsc(topicsSet: Set[String], kafkaParams: Map[String, String],checkPoint:String,sc:SparkContext,zkServers:String)(): StreamingContext = {
    val ssc = new StreamingContext(sc, Seconds(10))
    /* create direct kafka stream */
    val messages = createCustomDirectKafkaStream(ssc,kafkaParams,zkServers,"/kafka", topicsSet)
    val line = messages.map(_._2)
    val words= line.flatMap(row=>row.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    words.print()
    ssc
  }//setUp(ssc) ends
  /* createDirectStream() method overloaded */
  def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], zkHosts: String
                                    , zkPath: String, topics: Set[String]): InputDStream[(String, String)] = {
    val topic = topics.last //TODO only for single kafka topic right now
    val zkClient = new ZkClient(zkHosts, 30000, 30000)
    val storedOffsets = readOffsets(zkClient,zkHosts, zkPath, topic)
    logger.debug("偏移量"+storedOffsets)
    val kafkaStream = storedOffsets match {
      case None => // start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) => // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder
          , (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    // save the offsets
    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient,zkHosts, zkPath, rdd))
    kafkaStream
  }
  /*
  Read the previously saved offsets from Zookeeper
   */
  private def readOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, topic: String):
  Option[Map[TopicAndPartition, Long]] = {
    logger.info("Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        logger.info(s"Read offset ranges: ${offsetsRangesStr}")
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap
        logger.info("Done reading offsets from Zookeeper. Took " + stopwatch)
        Some(offsets)
      case None =>
        logger.info("No offsets found in Zookeeper. Took " + stopwatch)
        None
    }
  }

  private def saveOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, rdd: RDD[_]): Unit = {
    logger.info("Saving offsets to Zookeeper")
    val stopwatch = new Stopwatch()
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => logger.debug(s"Using ${offsetRange}"))
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.info("chandan Writing offsets to Zookeeper zkClient="+zkClient+"  zkHosts="+zkHosts+" zkPath="+zkPath+"  offsetsRangesStr:"+ offsetsRangesStr)
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
    logger.info("Done updating offsets in Zookeeper. Took " + stopwatch)
  }

}
