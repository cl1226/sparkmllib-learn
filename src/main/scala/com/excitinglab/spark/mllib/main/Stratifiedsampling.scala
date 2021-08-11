package com.excitinglab.spark.mllib.main

import com.excitinglab.spark.mllib.utils.SparkUtils

/**
 * 分层抽样
 * 将数据根据不同的特征分成不同的组，然后按特定条件从不同的组中获取样本
 * 通过RDD的sampleByKey和sampleByKeyExact
 * 参数：            含义
 * withReplacement  每次抽样是否有放回
 * fractions        控制不同key的抽样率
 * seed             随机数种子
 *
 * ((1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'))
 * fractions = Map(1 -> 0.4, 2 -> 0.6)
 * 抽样结果：
 * sampleByKey: 结果不一定准确，不过在样本足够大且要求一定效率的情况下，也可以
 * sampleByKeyExact:  (1, 'a'), (2, 'c'), (2, 'd') 与预想结果一致，但当样本数量比较大时，可能会耗时较久
 */
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
