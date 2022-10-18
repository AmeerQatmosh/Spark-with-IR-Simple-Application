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
      .reduceByKey(_+_)
      .sortByKey()
      .map(oi => oi._1 + "," + oi._2 + ","  )

    val stopWordsInput = sc.textFile("src/main/resources/DataFiles/doc*.txt")
    val stopWords = stopWordsInput.flatMap(x => x.split("\\r?\\n")).map(_.trim)
    val broadcastStopWords = sc.broadcast(stopWords.collect.toSet)
    val file = sc.wholeTextFiles("src/main/resources/DataFiles/doc*.txt").flatMap {
      case (path, text) =>
        text.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ")
          .split("""\W+""")
          .filter(!broadcastStopWords.value.contains(_)) map {
          // Create a tuple of (word, filePath)
          word => (word, path)
        }
    }.map {
      // Create a tuple with count 1 ((word, fileName), 1)
      case (w, p) => ((w, p.split("/")(6)), 1)}
      .reduceByKey {
      // Group all (word, fileName) pairs and sum the counts
      case (n1, n2) => n1 + n2
    }.map {
      // Transform tuple into (word, (fileName, count))
      case ((w, p), n) => (w, (p, n))
    }.groupBy {
      // Group by words
      case (w, (p, n)) => w
    }

    file.foreach(println)





    //println("Reading collection files: \n")
   // countRDD.collect().foreach(println)



  }

}








    //countRDD.saveAsTextFile("src/main/resources/Store/wholeInvertedIndex.txt")






