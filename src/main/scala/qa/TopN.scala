package qa

import org.apache.spark.{SparkConf, SparkContext}

object TopN {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf= new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Test on local")

    val sc=new SparkContext(conf)

    val text=sc.textFile("E:/topN.txt")
    val kvData=text.map(line=>{
      val kvSource=line.split(",")
      (kvSource(0).trim,kvSource(1).toInt)
    }).reduceByKey(_+_).sortBy(_._2,false);
//    kvData.foreach(line=>println(line))
    val myOrder=new Ordering[(String,Int)] {
      override def compare(x: (String, Int), y: (String, Int)) = {
        if(x._2>=y._2)
          1
        -1
      }
    }
    kvData.take(3).foreach(line=>println(line))
    kvData.takeOrdered(3)(myOrder).foreach(line=>println(line))

  }
}

