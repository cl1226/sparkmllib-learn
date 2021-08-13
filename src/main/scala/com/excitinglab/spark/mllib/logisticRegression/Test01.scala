package com.excitinglab.spark.mllib.logisticRegression

import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

case class Iris(calyxLength: Double, calyxWidth: Double, petalLength: Double, petalWidth: Double, classification: String)

/**
 * 使用mllib包里的LogisticRegressionWithLBFGS逻辑回归算法
 * LogisticRegressionWithLBFGS是基于算法L-BFGS。L-BFGS算法是对拟牛顿算法的一个改进。
 * L-BFGS算法的基本思想是：算法只保存并利用最近m此迭代的曲率信息来构造海森矩阵的近似矩阵
 */
object Test01 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LogisticRegression")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("error")

    import spark.implicits._

    val df = sc.textFile("E:\\workspace\\sparkmllib-learn\\data\\iris.data")
      .filter(StringUtils.isNotBlank(_))
      .map(x => {
        val array = x.split(",")
        Iris(array(0).toDouble, array(1).toDouble, array(2).toDouble, array(3).toDouble, array(4))
      }).toDF()

    val indexer = new StringIndexer().setInputCol("classification").setOutputCol("category")
    val ind = indexer.fit(df)
    val frame = ind.transform(df)

    val features = List("calyxLength", "calyxWidth", "petalLength", "petalWidth").map(frame.columns.indexOf(_))
    val categoryIndex = frame.columns.indexOf("category")
    val labeledPoint = frame.rdd.map(r => {
      LabeledPoint(r.getDouble(categoryIndex), Vectors.dense(features.map(r.getDouble(_)).toArray))
    })

    val array = labeledPoint.randomSplit(Array(0.8, 0.2), 1L)
    val train = array(0).cache()
    val test = array(1)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(train)

    val res = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    res.foreach(println)
  }

}
