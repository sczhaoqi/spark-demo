package qa

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ReadFile {
  case class Person(id: String, username: String,password:String) // 必须是顶级类
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf = new SparkConf().setAppName("SecondContacts").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc= SparkSession.builder().config(conf)
      .enableHiveSupport().getOrCreate()

    val json = sc.textFile("E:/person.txt")
    // 将其解析为特定的case class。使用flatMap，通过在遇到问题时返回空列表（None）
    // 来处理错误，而在没有问题时返回包含一个元素的列表（Some(_)）
    val mapper = new ObjectMapper();
    val result = json.flatMap(record => {
      try {
        Some(mapper.readValue(record, classOf[Person]))
      } catch {
        case e: Exception => None
      }})
    val jsos=ssc.read.json("E:/person.txt").toDF()
    jsos.show()
    jsos.createOrReplaceTempView("person")
    ssc.sql("select * from person").show()
  }
}