package qa

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf= new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Test on local")
    val spark=new SparkContext(conf)

    val text=spark.textFile("E:/word.txt")
    val word=text.flatMap(line=>line.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    word.foreach(line=>println(line))

    val sum=word.map(x=>x._1.toInt*x._2).reduce(_+_)

    println(sum)

    val sum2=text.flatMap(line=>line.split(" ")).map(x=>x.toInt).reduce(_+_)

    println(sum2)

    //注意:INT类型的取值范围
    val avg =word.aggregate((0.0,0))(
      (num,value)=>(num._1+value._1.toInt*value._2,num._2+value._2),
      (num1,num2)=>(num1._1+num2._1,num2._2+num1._2)
    )

    println("sum:"+avg._1+"num:"+avg._2+"平均数为:"+avg._1/avg._2.toDouble)

    //过滤大于500的数
    val filterWord=word.filter(line=>line._1.toDouble<500)

    filterWord.take(1).foreach(line=>println(line))

    //???????
    word.coalesce(1)
    println("分区大小:"+word.partitions.size)

    //join操作的实质 cogroup()
    val storeAddress = spark.parallelize(Seq((("Ritual"), "1026 Valencia St"), (("Philz"), "748 Van Ness Ave"),(("Philz"), "3101 24th St"), (("Starbucks"), "Seattle")))

    val storeRating = spark.parallelize(Seq((("Ritual"), 4.9), (("Philz"), 4.8)))
    val tjoin=storeAddress.join(storeRating)
    val ljoin=storeAddress.leftOuterJoin(storeRating)
    val rjoin=storeAddress.rightOuterJoin(storeRating)
    //普通join
    println("普通join")
    tjoin.foreach(line=>println(line))
    //leftouterjoin
    println("leftouterjoin")
    ljoin.foreach(line=>println(line))
    //rightouterjoin
    println("rightouterjoin")
    rjoin.foreach(line=>println(line))

    //排序
    val a = spark.parallelize(List("wyp", "iteblog", "com", "397090770", "test"))
    val b = spark.parallelize(List(3,1,9,12,4))
    val c = b.zip(a)
    val c1=c.sortByKey().collect
    c1.foreach(line=>println(line))
    //按照字典序排序key
    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.toString.compare(b.toString)
    }
    val c2=c.sortByKey().collect()
    c2.foreach(line=>println(line))
  }
}