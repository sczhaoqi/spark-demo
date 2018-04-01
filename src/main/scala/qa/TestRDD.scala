package qa

import org.apache.spark.{SparkConf, SparkContext}

object TestRDD {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf= new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Test on local")
    val spark=new SparkContext(conf)

    val data =Array(("A","B"),("A","C"),("B","C"))
    val plRdd=spark.parallelize(data)
    val rlRang=plRdd.mapValues(line=>1.0)
    val plv=plRdd.join(rlRang);
    plv.foreach(line=>println(line._2._1+""+line._2._1.size))
    val plrs=plv.flatMap {
      case (page, (link, rang)) => link.map(x => (x, rang / link.size))
    }
    plrs.foreach(line=>println(line))
    val plcm=plrs.reduceByKey(_+_)
    plcm.foreach(line=>println(line))
    val wplcm=plcm.mapValues(v=>v*0.85+0.15)
  }

}

