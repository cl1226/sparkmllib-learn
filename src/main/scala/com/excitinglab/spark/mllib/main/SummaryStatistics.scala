package com.excitinglab.spark.mllib.main

import com.excitinglab.spark.mllib.utils.SparkUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

/**
 * 概括统计，统计维度：
 * mean         每列均值
 * max          每列最大值
 * min          每列最小值
 * variance     每列方差
 * numNonzeros  每列非零向量个数
 *
 * [1.0, 2.0, 3.0]
 * [1.1, 2.1, 3.1]
 * [1.2, 2.2, 3.2]
 * mean = [1.1, 2.1, 3.1]
 * max = [1.2, 2.2, 3.2]
 * min = [1.0, 2.0, 3.0]
 * variance = [0.01, 0.01, 0.01]
 * numNonzeros = [3.0, 3.0, 3.0]
 */
object SummaryStatistics {

  def main(args: Array[String]): Unit = {
    val (spark, sc) = SparkUtils.env("Statistics", 2)

    val vector: RDD[Vector] = sc.parallelize(Seq(
      Vectors.dense(1.0, 2.0, 3.0),
      Vectors.dense(1.1, 2.1, 3.1),
      Vectors.dense(1.2, 2.2, 3.2)))

    import spark.implicits._

    val df = vector.map(x => {
      (x(0), x(1), x(2))
    }).toDF("c1", "c2", "c3")

    df.show()

    val summary = Statistics.colStats(vector)
    println(summary.mean)
    println(summary.max)
    println(summary.min)

    println(summary.variance)
    println(summary.numNonzeros)
  }

}
