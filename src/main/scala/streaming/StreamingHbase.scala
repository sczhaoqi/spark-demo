package streaming

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object StreamingHbase {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val zkServers="192.168.192.145,192.168.192.148,192.168.192.149"
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("hbase"))
    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 to 10000)
    //    rdd.foreachPartition(x => {
    //      val hbaseConf = HBaseConfiguration.create()
    //      hbaseConf.set("hbase.zookeeper.quorum", zkServers)
    //      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    //      hbaseConf.set("hbase.defaults.for.version.skip", "true")
    //      val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    //      val table = hbaseConn.getTable(TableName.valueOf("word"))
    //      x.foreach(value => {
    //        //一条一条的插入
    //        var put = new Put(Bytes.toBytes(value.toString))
    //        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(value.toString))
    //        table.put(put)
    //      })
    //    })


    //    val rdd2=sc.makeRDD(Array(1)).flatMap(_=>10001 to 20000)
    //    rdd2.map(value => {
    //      var put = new Put(value.toString.getBytes())
    //      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(value.toString))
    //      put
    //    }).foreachPartition(iterator => {
    //      var jobConf = new JobConf(HBaseConfiguration.create())
    //      jobConf.set("hbase.zookeeper.quorum", zkServers)
    //      jobConf.set("zookeeper.znode.parent", "/hbase")
    //      jobConf.setOutputFormat(classOf[TableOutputFormat])
    //      val table = new HTable(jobConf, TableName.valueOf("word"))
    //      import scala.collection.JavaConversions._
    //      table.put(seqAsJavaList(iterator.toSeq))
    //    })
    var jobConf = new JobConf(HBaseConfiguration.create())
    jobConf.set("hbase.zookeeper.quorum", zkServers)
    jobConf.set("zookeeper.znode.parent", "/hbase")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "word")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    val rdd3 = sc.makeRDD(Array(1)).flatMap(_ => 0 to 1000000)
    rdd3.map(x => {
      var put = new Put(Bytes.toBytes(x.toString))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)

    val readFile = sc.textFile("/path/to/file").map(x => x.split(","))
    val tableName = "table"
    readFile.foreachPartition{
      x=> {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", zkServers)
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, TableName.valueOf(tableName))
        myTable.setAutoFlush(false, false)//关键点1
        myTable.setWriteBufferSize(3*1024*1024)//关键点2
        x.foreach { y => {
          println(y(0) + ":::" + y(1))
          val p = new Put(Bytes.toBytes(y(0)))
          p.add("Family".getBytes, "qualifier".getBytes, Bytes.toBytes(y(1)))
          myTable.put(p)
        }
        }
        myTable.flushCommits()//关键点3
      }
    }

    //    此程序是使用了RDD的foreachPartition函数，在此程序中有三个比较关键的地方。
    //    关键点1_:将自动提交关闭，如果不关闭，每写一条数据都会进行提交，是导入数据较慢的做主要因素。
    //    关键点2:设置缓存大小，当缓存大于设置值时，hbase会自动提交。此处可自己尝试大小，一般对大数据量，设置为5M即可，本文设置为3M。
    //    关键点3:每一个分片结束后都进行flushCommits()，如果不执行，当hbase最后缓存小于上面设定值时，不会进行提交，导致数据丢失。
    //    注：此外如果想提高Spark写数据如Hbase速度，可以增加Spark可用核数量。
  }
}
