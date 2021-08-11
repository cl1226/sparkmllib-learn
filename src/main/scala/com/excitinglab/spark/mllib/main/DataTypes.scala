package com.excitinglab.spark.mllib.main

import com.excitinglab.spark.mllib.utils.SparkUtils
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Local Vector（本地向量）
 *  一个本地向量拥有从0开始的integer类型的索引以及double类型的值，它保存在单台机器上
 *    稠密（dense）
 *    稀疏（sparse）
 * Labeled Point（标记点）
 *  一种带有标签的本地向量，在监督学习中使用，由于标签是用双精度浮点类型来存储的，故可用于分类和回归模型
 * Local Matrix（本地矩阵）
 *  本地矩阵是包含行和列都是整数的指标
 *    稠密矩阵（dense matrix）
 *    稀疏矩阵（sparse matrix）
 * Distributed Matrix（分布式矩阵）
 * RowMatrix(行矩阵)
 * IndexedRowMatrix(标记行矩阵)
 * CoordinateMatrix(坐标矩阵)
 * BlockMatrix(分块矩阵)
 */
object DataTypes {

  def main(args: Array[String]): Unit = {
    val (spark, sc) = SparkUtils.env("DataTypes", 2)

    println("-----Local Vector-----")

    val dense = Vectors.dense(1.0, 2.0, 3.0)
    val sparse = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    sparse.toArray.foreach(println)

    println("-----Labeled Point-----")

    val pos = LabeledPoint(1.0, dense)
    val neg = LabeledPoint(2.0, sparse)

    println("-----Local Matrix-----")

    /**
     * [1.0, 2.0]
     * [3.0, 4.0]
     * [5.0, 6.0]
     */
    val dm = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    println(dm)

    /**
     * numRows = 3, numCols = 2, rowIndices = [0, 2, 1], colPtrs = [0, 1, 3]
     * colPtrs = [0, 1, 3] => 每列非零个数[1-0, 3-1] => [1, 2]
     * 0  9.0   0
     * 1  0     8.0
     * 2  0     6.0
     *    1     2
     * [9.0, 0]
     * [0, 8.0]
     * [0, 6.0]
     */
    val sm = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    println(sm)

    println("-----Row Matrix-----")
    val rdd = sc.parallelize(Seq(
      (1.0, 2.0, 3.0),
      (1.1, 2.1, 3.1),
      (1.2, 2.2, 3.2)
    ))
    val vector: RDD[Vector] = rdd.map(r => {
      Vectors.dense(r._1.toString.toDouble, r._2.toString.toDouble, r._3.toString.toDouble)
    })

    val matrix = new RowMatrix(vector)
    println(matrix.numCols())
    println(matrix.numRows())
    matrix.rows.foreach(println)

    println("-----Coordinate Matrix-----")
    val entry1 = MatrixEntry(0, 1, 0.5)
    val entry2 = MatrixEntry(2, 2, 1.8)
    val matrixEntryRDD = sc.parallelize(Array(entry1, entry2))
    val coordinateMatrix = new CoordinateMatrix(matrixEntryRDD)
    coordinateMatrix.entries.foreach(println)

    // TODO

  }

}
