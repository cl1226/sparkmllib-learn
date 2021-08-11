package com.excitinglab.spark.mllib.main

import com.excitinglab.spark.mllib.utils.SparkUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

/**
 * 相关性
 * spark支持两种相关系数：pearson（皮尔逊相关系数）、spearman（斯皮尔曼登记相关系数）。
 * 反映的是变量之间的相关密切程度的j统计指标。
 * 相关系数绝对值越大，相关性越大，0表示不想管，[-1~0)表示负相关，(0~1]表示正相关
 * 相关序列：
 * 要求：两个序列的个数相同、分区相同
 * 相关矩阵：
 * 相关矩阵也叫相关系数矩阵，是由矩阵各列间的相关系数构成的。相关矩阵第i行第j列的元素是原矩阵第i行第j列的相关系数
 *
 */
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
