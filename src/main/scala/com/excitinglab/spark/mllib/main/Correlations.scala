package com.excitinglab.spark.mllib.main

import com.excitinglab.spark.mllib.utils.SparkUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

object Correlations {

  def main(args: Array[String]): Unit = {
    val (spark, sc) = SparkUtils.env("Correlations", 2)

    val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))
    val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))

    val correlation = Statistics.corr(seriesX, seriesY, "pearson")

    println(s"Correlation is: $correlation")

    val data = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    )

    val matrix = Statistics.corr(data, "pearson")
    println(matrix.toString)
  }

}
