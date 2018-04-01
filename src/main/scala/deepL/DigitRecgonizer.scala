package deepL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes,NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object DigitRecgonizer {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    val conf= new SparkConf()
    conf.setMaster("local")
    conf.setAppName("DigitRecgonizer")
    val spark=new SparkContext(conf)

    //过滤第一行的header
    val trainData =spark.textFile("E:/train.csv")
    val records = trainData.map(line => line.split(","))
    val data = records.map{ r =>
      val label = r(0).toInt
      val features = r.slice(1, r.size).map(p => p.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }
    val nbModel = NaiveBayes.train(data)
    val nbTotalCorrect = data.map { point =>
      if (nbModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val numData = data.count()
    val nbAccuracy = nbTotalCorrect / numData
    val unlabeledData = spark.textFile("E:/test.csv")
    val unlabeledRecords = unlabeledData.map(line => line.split(","))
    val features = unlabeledRecords.map{ r =>
      val f = r.map(p => p.toDouble)
      Vectors.dense(f)
    }
    val predictions = nbModel.predict(features).map(p => p.toInt)
    predictions.repartition(1).saveAsTextFile("E:/out.txt")
  }

}

