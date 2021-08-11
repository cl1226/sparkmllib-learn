package com.excitinglab.spark.mllib.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkUtils {

  def env(name: String, vcores: Int): (SparkSession, SparkContext) = {
    val spark = SparkSession.builder()
      .appName(name)
      .master(s"local[$vcores]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("error")

    (spark, sc)
  }

}
