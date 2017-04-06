import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

package object neartime {

  val _i = (x : Array[Byte]) => Bytes.toInt(x)
  val _s = (x : Array[Byte]) => Bytes.toString(x)
  val _h = (x : Array[Byte]) => Bytes.toHex(x)
  val _b = (x : String) => Bytes.toBytes(x)

  def setupSpark = {
    val conf = new SparkConf().setAppName("LoadTestEventsIntoHBase").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc
  }

}
