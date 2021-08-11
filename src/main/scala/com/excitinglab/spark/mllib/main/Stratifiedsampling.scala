package com.excitinglab.spark.mllib.main

import com.excitinglab.spark.mllib.utils.SparkUtils

object Stratifiedsampling {

  def main(args: Array[String]): Unit = {
    val (spark, sc) = SparkUtils.env("Stratifiedsampling", 4)

    val data = sc.parallelize(
      Seq((1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'))
    )

    val fractions = Map(1 -> 0.4, 2 -> 0.6)

    val approxSample = data.sampleByKey(false, fractions)
    val exactSample = data.sampleByKeyExact(false, fractions)

    approxSample.foreach(println)

    println("------------")

    exactSample.foreach(println)
  }

}
