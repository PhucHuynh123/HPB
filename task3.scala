package task3

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task 3")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // Read data file log
    val lineRDD = sc.textFile("file:///share/FPT-2018-12-02.log")

    // caculate HIT, MISS, HIT1
    val hitRDD = lineRDD.filter(line => line.contains("HIT ")).cache()
    val missRDD = lineRDD.filter(line => line.contains("MISS ")).cache()
    val hit1RDD = lineRDD.filter(line => line.contains("HIT1 ")).cache()

    val hitCount = hitRDD.count()
    val missCount = missRDD.count()
    val hit1Count = hit1RDD.count()

    println(s"Number of HIT requests: $hitCount")
    println(s"Number of MISS requests: $missCount")
    println(s"Number of HIT1 requests: $hit1Count")

    // caculate HitRate
    val hitRate = (hitCount + hit1Count).toDouble / (hitCount + hit1Count + missCount)
    println(s"HitRate of the system: $hitRate")

    // Find ISP has HitRate best
    val ispData = lineRDD.map(line => (line.split(" ")(1), line)).distinct().cache()
    val ispsHitRate = ispData.mapValues(line => {
      if (line.contains("HIT ") || line.contains("HIT1 ")) 1 else 0
    }).reduceByKey(_ + _).collect()

    val bestISP = ispsHitRate.maxBy(_._2)._1
    println(s"Best ISP with the highest HitRate: $bestISP")

    spark.stop()
  }
}
