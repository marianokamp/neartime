package neartime

import neartime.GenerateTestData.Event
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import collection.JavaConverters._



object ProcessNeartimeProcessEvents extends HBaseApp{

  val dbg = true
  val EMPTY: Array[Byte]= Array(0.toByte)

  def debug(message: String): Unit = {
    if (dbg)
      println(message)
  }

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = setupSpark

    val connection: Connection = openHBaseConnection

    processEvents(GenerateTestData.createTestData(10), connection)

    closeHBaseConnection(connection)

    sc.stop()
  }

  def processEvents(events: List[Event], connection: Connection): Unit = {

    val eventsTable: Table = setupTable(connection, "events", _.addFamily(new HColumnDescriptor(Bytes.toBytes("ev"))))
    val processesTable: Table = setupTable(connection, "processes", _.addFamily(new HColumnDescriptor(Bytes.toBytes("ps"))))
    val processSecondaryIndexTable: Table = setupTable(connection, "processes-index", _.addFamily(new HColumnDescriptor(Bytes.toBytes("pi"))))
    val orphanedEventsTable: Table = setupTable(connection, "orphaned-events", _.addFamily(new HColumnDescriptor(Bytes.toBytes("oe"))))


    // FIXME this currently only checks for a single orphan, but what about multiple orphans, i.e. multiple children that share the same missing parent process?

    def reconcile(processesTable: Table, orphanedEventsTable: Table, ev: Event) = {
      val lookupOrphanRes = orphanedEventsTable.get(new Get(Bytes.toBytes(ev.id)).setMaxVersions(1).addFamily(Bytes.toBytes("oe")))

      if (!lookupOrphanRes.isEmpty) {

        // Found an orphan

        // - Get the orphaned-id from the orphaned events
        val orphanedId = lookupOrphanRes.getValue(Bytes.toBytes("oe"), Bytes.toBytes("orphaned-id"))
        debug("orphanedId:" + _i(orphanedId))

        // - Get the process to merge (merge_source) using the orphaned-id

        val mergeSourceProcessId = lookupProcessByEvent(_i(orphanedId))

        // - Get the process to be merged (merge_target) using the current ev.id

        val mergeTargetProcessId = lookupProcessByEvent(ev.id)

        // - Get the affected ids from the merge_source (columns of merge_source)

        val mergeSourceProcessRes = processesTable.get(new Get(mergeSourceProcessId).addFamily(Bytes.toBytes("ps")))

        if (mergeSourceProcessRes.isEmpty)
          throw new IllegalStateException("Oops. Merge source process does not exists for id: "+Bytes.toString(mergeSourceProcessId))

        val affectedEventIds = mergeSourceProcessRes.getFamilyMap(Bytes.toBytes("ps")).asScala.map {
          case (q, v) =>
            debug("xq=" + _i(q))
            q
        }

        // - Put the affected ids into merge_target (Process)

        val addAffectedIdsToTargetProcessPut = new Put(mergeTargetProcessId)
        affectedEventIds.foreach(eventId => addAffectedIdsToTargetProcessPut.addColumn(Bytes.toBytes("ps"), eventId, EMPTY))
        processesTable.put(addAffectedIdsToTargetProcessPut)

        // - Delete merge_source (do it first to reduced stale issue / concurrency issue
        processesTable.delete(new Delete(mergeSourceProcessId))

        // - Update process events index for affected ids, now pointing to the merge_target

        affectedEventIds.foreach(affectedEventId => putProcessIndex(_i(affectedEventId), mergeTargetProcessId))

        // -
      }

      // - Delete orphaned event for rowkey = ev.id
      orphanedEventsTable.delete(new Delete(Bytes.toBytes(ev.id)))
    }

    def lookupProcessByEvent(eventId: Int) = {
      val mergeTargetRes = processSecondaryIndexTable.get(new Get(Bytes.toBytes(eventId)).addFamily(Bytes.toBytes("pi")))
      if (mergeTargetRes.isEmpty)
        throw new IllegalStateException("Oops. Merge Target")
      val mergeTargetProcessId = mergeTargetRes.getValue(Bytes.toBytes("pi"), Bytes.toBytes("process-id"))
      mergeTargetProcessId
    }

    def putProcessIndex(eventId: Int, processRowKey: Array[Byte]) = {
      val newIndexPut = new Put(Bytes.toBytes(eventId)).addColumn(Bytes.toBytes("pi"), Bytes.toBytes("process-id"), processRowKey)
      processSecondaryIndexTable.put(newIndexPut)
      debug("Created process index for event " + eventId + " pointing to process " + Bytes.toString(processRowKey))
    }

    def putProcess(eventId: Int, processRowKey: Array[Byte]) = {
      processesTable.put(new Put(processRowKey).addColumn(Bytes.toBytes("ps"), Bytes.toBytes(eventId), EMPTY))
      debug("Created/Updated process " + Bytes.toInt(processRowKey) + " pointing to event " + eventId)
    }

    def putProcessAndIndex(eventId: Int, processRowKey: Array[Byte]) = {
      putProcess(eventId, processRowKey)
      putProcessIndex(eventId, processRowKey)
    }

    def putEvent(eventId: Int, predecessorId: Int) = {
      val eventPut = new Put(Bytes.toBytes(eventId))
      if (predecessorId > -1)
        eventPut.addColumn(Bytes.toBytes("ev"), Bytes.toBytes("predecessor-id"), Bytes.toBytes(predecessorId))
      eventPut.addColumn(Bytes.toBytes("ev"), Bytes.toBytes("text"), Bytes.toBytes("llll"))
      eventsTable.put(eventPut)
    }

    def processNewEvent(ev: Event) = {

      var checkForOrphans = true

      debug("processNewEvent for event " + ev.id + ": " + ev)

      // insert event into events
      putEvent(ev.id, ev.predecessorId)

      // event is root
      if (ev.predecessorId > -1) {
        val indexLookupRes = processSecondaryIndexTable.get(new Get(Bytes.toBytes(ev.predecessorId)).addFamily(Bytes.toBytes("pi")))

        if (!indexLookupRes.isEmpty) {
          // Found index for predecessor
          // Find process and maintainProcessReference

          val processIdKey = indexLookupRes.getValue(Bytes.toBytes("pi"), Bytes.toBytes("process-id"))

          val process = processesTable.get(new Get(processIdKey).addFamily(Bytes.toBytes("ps")))

          if (!process.isEmpty) {
            // process found, maintain reference to this event then
            val processRowKey = process.getRow

            if (dbg) {
              val qvs = process.getFamilyMap(_b("ps")).asScala.foreach {
                case (q, v) => debug("q=" + _i(q))
              }
            }

            putProcessAndIndex(ev.id, processRowKey)
            // first ev.id could be step or system, if unique for one process
          } else {
            // now write a new process, pointing to this event
            val processRowKey = Bytes.toBytes(createUniqueId)
            putProcessAndIndex(ev.id, processRowKey)
          }

        } else {
          debug("Haven't found index for predecessor " + ev.predecessorId + " Predecessor not created yet?")

          // Capture orphaned event
          val orphanPut = new Put(Bytes.toBytes(ev.predecessorId))
          orphanPut.addColumn(Bytes.toBytes("oe"), Bytes.toBytes("orphaned-id"), Bytes.toBytes(ev.id))
          orphanedEventsTable.put(orphanPut)

          // Create index and Process

          val rowKey = Bytes.toBytes(createUniqueId)
          putProcessAndIndex(ev.id, rowKey)

          // As we are just creating the orphan for this event,
          // there is no need to check for it, after writing this event
          checkForOrphans = false

        }
      } else {
        // Deal with an unspecified predecessor, otherwise called root
        val rowKey = Bytes.toBytes(createUniqueId)
        putProcessAndIndex(ev.id, rowKey)
      }

      // ----

      // Now check if this event resolves an orphan?

      if (checkForOrphans)
        reconcile(processesTable, orphanedEventsTable, ev)

        // ---
    }

    val started = System.currentTimeMillis()

    events.foreach(processNewEvent)

    println("Before close " + (System.currentTimeMillis() - started))

    closeTable(eventsTable)
    closeTable(processesTable)
    closeTable(processSecondaryIndexTable)
    closeTable(orphanedEventsTable)

    println("After close "+(System.currentTimeMillis()-started))
  }


}
