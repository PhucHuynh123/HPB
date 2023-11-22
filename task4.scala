package task4

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task 4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // Read data file log
    val lineRDD = sc.textFile("file:///share/FPT-2018-12-02.log")

    // Cache/Persistence
    val ipDataRDD = lineRDD.map(line => (line.split(" ")(1), line)).distinct()
    ipDataRDD.cache() 

    val startWithoutCache = System.nanoTime()
    val uniqueIPsWithoutCache = ipDataRDD.map(_._1).distinct().collect()
    val endWithoutCache = System.nanoTime()

    val startWithCache = System.nanoTime()
    val uniqueIPsWithCache = ipDataRDD.map(_._1).distinct().collect()
    val endWithCache = System.nanoTime()

    println(s"Time taken without caching: ${(endWithoutCache - startWithoutCache) / 1e6} milliseconds")
    println(s"Time taken with caching: ${(endWithCache - startWithCache) / 1e6} milliseconds")

    // Shared variables (Accumulators)
    val accumulator = sc.longAccumulator("UniqueIPs")
    ipDataRDD.foreach(_ => accumulator.add(1))

    println(s"Number of unique IPs using accumulator: ${accumulator.value}")

    spark.stop()
  }
}
