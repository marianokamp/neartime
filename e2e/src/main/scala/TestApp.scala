import org.apache.spark.{SparkConf, SparkContext}

object TestApp {

  def main(args: Array[String]): Unit = {

    println("------------- start! -------------------------- "+args.toString)
    val words = "one two three one two three four five six"
    val conf = new SparkConf().setAppName("Test")

    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(words.split(" "))
    val wordsOnes = rdd.map((_, 1))
    println(sc.getConf.toDebugString)
    println("-------------- wordOnes ----------------------")
    val wordFrequencies = wordsOnes.reduceByKey(_+_)
    println("--------- before foreach ---------------------")
    wordFrequencies.foreach(s => println("s="+s))
    println("------------- done! --------------------------"+wordFrequencies.count())
    sc.stop()
  }
}
