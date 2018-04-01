package qa

import org.apache.spark.{SparkConf, SparkContext}

object SecondContacts {

  def main(args: Array[String]): Unit = {
    //uid,num
    def Friends_Sort(friends: List[(Int, Int)]) : List[(Int, Int)]   = {
      friends.sortBy(-_._2)
    }
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf = new SparkConf().setAppName("SecondContacts").setMaster("local")
    val sc = new SparkContext(conf)
    //uid fid
    val List_Friends = sc.textFile("E:/friends.txt").cache()

    List_Friends.foreach(line=>println(line))
    //(uid,fid) uid标示两个fid的中间好友
    val Friends_Pair=List_Friends.map(line=>{
      val uf=line.split(",")
      (uf(0).toInt,uf(1).toInt)
    })

    Friends_Pair.foreach(line=>println(line))

    //(uid,(fid,fid))
    val Self_Join = Friends_Pair.join(Friends_Pair)

    Self_Join.foreach(line=>println(line))
    //过滤自身(fid,fid)
    val Friends_All = Self_Join.map(elem => elem._2).filter(elem => elem._1 != elem._2)
    Friends_All.foreach(line=>println(line))

    //返回在RDD中出现，并且不在otherRDD中出现的元素，去重
    // (fid,fid) 过滤 (uid,fid) 过滤直接好友,含义为二度好友键值对
    val Mutual_Frnd = Friends_All.subtract(Friends_Pair)

    Mutual_Frnd.foreach(line=>println(line))
    //二度好友键值对,1  ((fid,fid),1)
    val Pair_Frnd = Mutual_Frnd.map(MutualFrnd_Pair => (MutualFrnd_Pair, 1))
    Pair_Frnd.foreach(line=>println(line))
    //找出所有好友,并实现相关性的排序
    //reduceByKey ((fid1,fid2),num)
    //map (fid1,(fid2,num))
    //groupByKey (fid1,{(fid2,num),(fid3,num)})
    //map (fid1,((fid,num大),```,(fid,num小)))
    //map " fid \t fid,num
    val Recommendation = Pair_Frnd
      .reduceByKey((a, b) => a + b)
      .map(elem => (elem._1._1, (elem._1._2, elem._2)))
      .groupByKey()
      .map(triplet => (triplet._1, Friends_Sort(triplet._2.toList)))
      .map(triplet => triplet._1.toString + "\t" + triplet._2.map(x=>x.toString).toArray.mkString(";"))
      .collect.mkString("\r\n")
    Recommendation.foreach(line=>print(line))
  }
}