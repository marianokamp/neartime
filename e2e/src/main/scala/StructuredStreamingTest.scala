import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime


object StructuredStreamingTest {

  def main(args: Array[String]): Unit = {

    val spark = setupSpark("memory")

    import spark.implicits._

    val linesDf = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "topic")
      .option("startingOffsets", "earliest")
      .load()

    val lines = linesDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]


    val wordCounts = lines.flatMap(_._2.split(" ")).groupBy("value").count()
    print("---------------------------------------------- Word Counts=" + wordCounts)
    //Thread.sleep(20000)



    println("------------------------- 4 -------------------------")

    if (true) {
      val query = wordCounts
        .writeStream
        .format("memory")
        .queryName("word_counts")
        .outputMode("complete")
        .trigger(ProcessingTime("3 seconds"))
        .start()
      println("------------------------- 6 -------------------------")

      println("------------------------- 6.1 -------------------------")

      val results = spark.sqlContext.sql("SELECT * FROM word_counts")

      Thread.sleep(40000)
      println("------------------------- 6.3 -------------------------")

      println("------------------------- 6.5 -------------------------" + results.count())
      results.foreach { row =>
        println(s"----------------------------------------------------- row=$row")
      }


      println("------------------------- 7 -------------------------")
      println("------------------------- 7 -------------------------")
      println("------------------------- 7 -------------------------")

      println("------------------------- 8 -------------------------")


      query.stop()
    }
    else {
      println("------------------------- 7 -------------------------")
      println("------------------------- 7 -------------------------")
      println("------------------------- 7 -------------------------")
      val query = wordCounts
        .writeStream
        .format("console")
        .queryName("word_counts")
        .outputMode("complete")
        .trigger(ProcessingTime("3 seconds"))
        .start()

      println("------------------------- 7.2 -------------------------")
      println("------------------------- 7.2 -------------------------")
      println("------------------------- 7.2 -------------------------")

    }

    Thread.sleep(40000)
    spark.stop()
  }

  def setupSpark(appName: String = "StructuredStreamingTest") = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

    spark
  }


}
