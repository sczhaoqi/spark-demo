package qa

import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf= new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Test on local")
    val spark=new SparkContext(conf)

    val text=spark.textFile("E:/word2.txt")
    val kvWord=text.map(line=> {
      val kv = line.split(" ")
      (kv(0),kv(1))
    }).cache()

    val combinWord=kvWord.combineByKey(
      (v)=>(v,1),
      (acc:(String,Int),v)=>(acc._1+";"+v,acc._2+1),
      (acc1:(String,Int),acc2:(String,Int))=>(acc1._1,acc1._2+acc2._2)
    )
    combinWord.foreach(line=>println(line))

    kvWord.reduceByKey((x, y) => x + y) // 默认并行度

    kvWord.reduceByKey((x, y) => x + y,10) // 自定义并行度

  }
}