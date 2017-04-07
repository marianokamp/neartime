package neartime

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import GenerateTestData.Event
import neartime.ProcessNeartimeProcessEvents.{closeHBaseConnection, openHBaseConnection}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import collection.JavaConverters._
import scala.collection.mutable

class NeartimeTestSuite extends FunSuite with BeforeAndAfterAll {


  val sc: SparkContext = setupSpark
  val connection: Connection = openHBaseConnection

  override protected def afterAll(): Unit = {

    closeHBaseConnection(connection)
    sc.stop()

  }


  test("Create single event") {

    val ev = Event(id = 10, predecessorId = 20, text = "xyz")
    ProcessNeartimeProcessEvents.processEvents(List(ev), connection)

    assert(countRows("processes", "ps") == 1)
    assert(countRows("processes-index", "pi") == 1)
    assert(countRows("events", "ev") == 1)

    val pi = getRecordAsMap("processes-index", "pi", Bytes.toBytes(10)).map{case(q,v) => (_s(q), _s(v))}
    assert(pi.size == 1)
    val processId = pi("process-id")

    val p = getRecordAsMap("processes", "ps", Bytes.toBytes(processId)).map{case(q,v) => (_i(q), v)}
    assert(p.size == 1)
    assert(p.contains(10))

  }

  test("Create two connected events in the proper order") {

    val ev1 = Event(id = 1, predecessorId = -1, text = "xyz")
    val ev2 = Event(id = 2, predecessorId = 1, text = "xyz")
    ProcessNeartimeProcessEvents.processEvents(List(ev1, ev2), connection)

    assert(countRows("events", "ev") == 2)
    assert(countRows("processes", "ps") == 1)
    assert(countRows("processes-index", "pi") == 2)

  }

  test("Create two connected events in the wrong order should be reconciled") {

    val ev1 = Event(id = 1, predecessorId = -1, text = "xyz")
    val ev2 = Event(id = 2, predecessorId =  1, text = "xyz")

    ProcessNeartimeProcessEvents.processEvents(List(ev2, ev1), connection)

    assert(countRows("events", "ev")          == 2)
    assert(countRows("processes", "ps")       == 1)
    assert(countRows("processes-index", "pi") == 2)

    val ev1Process = Bytes.toString(getValue(Bytes.toBytes(1), "processes-index", "pi", "process-id"))
    val ev2Process = Bytes.toString(getValue(Bytes.toBytes(2), "processes-index", "pi", "process-id"))

    assert(ev1Process == ev2Process)

  }

  test("Create five connected events that miss the middle, then reconcile") {

    val ev1 = Event(id = 1, predecessorId = -1, text = "xyz")
    val ev2 = Event(id = 2, predecessorId =  1, text = "xyz")
    val ev3 = Event(id = 3, predecessorId =  2, text = "xyz")
    val ev4 = Event(id = 4, predecessorId =  3, text = "xyz")
    val ev5 = Event(id = 5, predecessorId =  4, text = "xyz")


    ProcessNeartimeProcessEvents.processEvents(List(ev1, ev2, ev4, ev5), connection)

    assert(countRows("events", "ev")          == 4)
    assert(countRows("processes", "ps")       == 2)
    assert(countRows("processes-index", "pi") == 4)

    val ev1Process = Bytes.toString(getValue(Bytes.toBytes(1), "processes-index", "pi", "process-id"))
    val ev4Process = Bytes.toString(getValue(Bytes.toBytes(4), "processes-index", "pi", "process-id"))

    // Assert broken chain of events
    assert(ev1Process != ev4Process)

    ProcessNeartimeProcessEvents.processEvents(List(ev1, ev2, ev4, ev5, ev3), connection)

    assert(countRows("events", "ev")          == 5)
    assert(countRows("processes", "ps")       == 1)
    assert(countRows("processes-index", "pi") == 5)

    val allProcessIds = List.range(1, 5).map(id => Bytes.toString(getValue(Bytes.toBytes(1), "processes-index", "pi", "process-id")))
    assert(allProcessIds.distinct.size == 1)


  }

  //test("Create three connected events with the third event doing an implicit reconciliation")

  private def getValue(id: Array[Byte], tableName: String, familyName: String, qualifier: String) = {
    val value = connection.getTable(TableName.valueOf(tableName)).get(new Get(id)).getValue(Bytes.toBytes(familyName), Bytes.toBytes(qualifier))
    value
  }

  private def countRows(tableName: String, family: String) = {
    val scanner = connection.getTable(TableName.valueOf(tableName)).getScanner(_b(family))
    var count = 0
    while (scanner.next() != null) count += 1
    scanner.close()
    count
  }

  def getRecordAsMap(tableName: String, family: String, rowKey: Array[Byte]): mutable.Map[Array[Byte], Array[Byte]] = {
    val res = connection.getTable(TableName.valueOf(tableName)).get(new Get(rowKey).setMaxVersions(1).addFamily(_b(family)))
    if (res.isEmpty)
      throw new IllegalStateException
    res.getFamilyMap(_b(family)).asScala
  }

}

