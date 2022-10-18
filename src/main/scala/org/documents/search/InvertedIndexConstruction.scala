package org.documents.search
import scala.io.Source
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object InvertedIndexConstruction {
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

    val read = sc.textFile("src/main/resources/DataFiles/doc*.txt")
    val countRDD = read
      .flatMap(line => line.split(" "))
      .map(word => ((word, 1)))
      .reduceByKey((x, y) => (x + y))
      .reduceByKey(_ + _)
      .sortByKey()
      .map(oi => oi._1 + "," + oi._2 + ","  )


    println("Reading collection files: \n")
    countRDD.collect().foreach(println)



  }

}








    //countRDD.saveAsTextFile("src/main/resources/Store/wholeInvertedIndex.txt")






