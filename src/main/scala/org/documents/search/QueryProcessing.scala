package org.documents.search

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object QueryProcessing {
  def main(args: Array[String]): Unit = {

    // code segment used to prevent excessive logging
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Collection Files Word Count")

    // creating spark context
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val read = sc.textFile("src/main/resources/output/wholeInvertedIndex.txt")
  }

}
