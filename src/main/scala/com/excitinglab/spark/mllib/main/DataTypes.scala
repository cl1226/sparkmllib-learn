package com.excitinglab.spark.mllib.main

import com.excitinglab.spark.mllib.utils.SparkUtils
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataTypes {

  def main(args: Array[String]): Unit = {
    val (spark, sc) = SparkUtils.env("DataTypes", 2)

    val rdd = sc.parallelize(Seq(
      (1.0, 2.0, 3.0),
      (1.1, 2.1, 3.1),
      (1.2, 2.2, 3.2)
    ))

    println(rdd.partitions.length)

    val vector: RDD[Vector] = rdd.map(r => {
      Vectors.dense(r._1.toString.toDouble, r._2.toString.toDouble, r._3.toString.toDouble)
    })

    val matrix = new RowMatrix(vector)

    println(matrix.numCols())
    println(matrix.numRows())

    matrix.rows.foreach(println)

    import spark.implicits._

    val df = matrix.rows.map(x => {
      (x(0).toDouble, x(1).toDouble, x(2).toDouble)
    }).toDF("c1", "c2", "c3")

    df.show()
  }

}
