import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TestStreaming {


  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("TestStreaming").getOrCreate()

    import spark.implicits._

    def setupSpark = {
      val conf = new SparkConf().setAppName("LoadTestEventsIntoHBase").setMaster("local[*]")
      val sc = new SparkContext(conf)
      sc
    }



    val linesDf = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "topic")
      .load()

    val linesDf2 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "topic")
      .load()

    val lines = linesDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val lines2 = linesDf2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val wordCounts = lines.flatMap(_._2.split(" ")).groupBy("value").count()
    val wordCounts2: org.apache.spark.sql.DataFrame = lines2.flatMap(_._2.split(" ")).groupBy("value").count()

    println("------------------------- 4 -------------------------")
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
    println("------------------------- 6 -------------------------")

    val writeBack: ListBuffer[String] = ListBuffer[String]()

    val writer = new ForeachWriter[Row] {

      println("ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
      override def process(row: Row): Unit = {
        println("---------------------------------------_process: value = "+row)
        println("_process. value.getClass = "+row.getClass)
        writeBack.append("Prcess: "+row)
      }

      override def close(errorOrNull: Throwable): Unit = println("-------------------- Now closing partition errorOrNull:"+errorOrNull)

      override def open(partitionId: Long, version: Long): Boolean = {
        println("Opening partition: "+partitionId+" version: "+version)
        true
      }

    }

    val query2 = wordCounts2.writeStream.foreach(writer).outputMode("complete").start()

    println("------------------------- 6.5 -------------------------")

    while (true) {
      //println("Query explain="+query.explain())
      //println("Query explain(e)="+query.explain(true))
      //println("last process="+query.lastProgress)
      println("\nisActive="+query.isActive+ " "+query2.isActive)
      println("status="+query.status+ " " + query2.status)
      println("recentProgress.length="+query.recentProgress.length)
      println("runId="+query.runId+" "+query2.runId)
      //println("recentProgress="+query.recentProgress.mkString("\n\t"))
      println("------ Response:\n "+writeBack.mkString("\n"))
      Thread.sleep(90000)
    }
    println("------------------------- 9 -------------------------")
    query.awaitTermination()


  }

  def main2(args: Array[String]): Unit = {

    val spark = SparkSession
        .builder
        .appName("TestStreaming").getOrCreate()

    import spark.implicits._

    val lines = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    println("------------------------- 4 -------------------------")
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    println("------------------------- 6 -------------------------")
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    println("------------------------- 8 -------------------------")

    query.awaitTermination()
    println("------------------------- 9 -------------------------")

  }
}