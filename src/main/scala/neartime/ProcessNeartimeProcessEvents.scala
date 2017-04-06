package neartime

import neartime.GenerateTestData.Event
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import collection.JavaConverters._



object ProcessNeartimeProcessEvents extends HBaseApp{

  val dbg = true

  def debug(message: String) = {
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

  def processEvents(events: List[Event], connection: Connection) = {

    val eventsTable: Table = setupTable(connection, "events", (_.addFamily(new HColumnDescriptor(Bytes.toBytes("ev")))))
    val processesTable: Table = setupTable(connection, "processes", (_.addFamily(new HColumnDescriptor(Bytes.toBytes("ps")))))
    val processSecondaryIndexTable: Table = setupTable(connection, "processes-index", (_.addFamily(new HColumnDescriptor(Bytes.toBytes("pi")))))
    val orphanedEventsTable: Table = setupTable(connection, "orphaned-events", (_.addFamily(new HColumnDescriptor(Bytes.toBytes("oe")))))


    // FIXME this currently only checks for a single orphans, but what about multiple orphans?

    def reconcile(processesTable: Table, orphanedEventsTable: Table, ev: Event) = {
      val lookupOrphanRes = orphanedEventsTable.get(new Get(Bytes.toBytes(ev.id)).setMaxVersions(1).addFamily(Bytes.toBytes("oe")))


      if (!lookupOrphanRes.isEmpty) {

        // Found an orphan
        // Now do the following:

        // - Get the orphaned-id from the orphaned events
        val orphanedId = lookupOrphanRes.getValue(Bytes.toBytes("oe"), Bytes.toBytes("orphaned-id"))
        debug("orphanedId:" + _i(orphanedId))

        // - Get the process to merge (merge_source) using the orphaned-id

        //val sourceProcessIdRes = processSecondaryIndexTable.get(new Get(orphanedId).setMaxVersions(1).addFamily(Bytes.toBytes("pi")))
        //if (sourceProcessIdRes.isEmpty)
        //  throw new IllegalStateException("Oops. Issue looking up source process id for "+_i(orphanedId))
        //val sourceProcessId =

        /*
        val mergeSourceIndexRes = processSecondaryIndexTable.get(new Get(orphanedId).setMaxVersions(1).addFamily(Bytes.toBytes("pi")))
        if (mergeSourceIndexRes.isEmpty)
          throw new IllegalStateException("Oops. Merge Source "+_i(orphanedId))
        val mergeSourceId = mergeSourceIndexRes.getRow*/

        val mergeSourceProcessId = lookupProcessByEvent(_i(orphanedId))

        // - Get the process to be merged (merge_target) using the current ev.id

        val mergeTargetProcessId = lookupProcessByEvent(ev.id)

        // - Get the affected ids from the merge_source (columns of merge_source)

        val mergeSourceProcessRes = processesTable.get(new Get(mergeSourceProcessId).setMaxVersions(1).addFamily(Bytes.toBytes("ps")))

        if (mergeSourceProcessRes.isEmpty)
          throw new IllegalStateException("Oops. Merge source process does not exists for id: "+Bytes.toString(mergeSourceProcessId))

        val affectedEventIds = mergeSourceProcessRes.getFamilyMap(Bytes.toBytes("ps")).asScala.map {
          case (q, v) => {
            println("xq=" + _i(q) + " v=" + _i(v))
            q
          }
        }

        // - Put the affected ids into merge_target (Process)

        val addAffectedIdsToTargetProcessPut = new Put(mergeTargetProcessId)
        affectedEventIds.foreach(eventId => addAffectedIdsToTargetProcessPut.addColumn(Bytes.toBytes("ps"), eventId, eventId))
        processesTable.put(addAffectedIdsToTargetProcessPut)

        // - Delete merge_source (do it first to reduced stale issue / concurrency issue
        processesTable.delete(new Delete(mergeSourceProcessId))

        // - Update process events index for affected ids, now pointing to the merge_target

        affectedEventIds.foreach(affectedEventId => updateProcessIndex(_i(affectedEventId), mergeTargetProcessId))

        // -

      }

      // - Delete orphaned event for rowkey = ev.id
      orphanedEventsTable.delete(new Delete(Bytes.toBytes(ev.id)))
    }

    def lookupProcessByEvent(eventId: Int) = {
      val mergeTargetRes = processSecondaryIndexTable.get(new Get(Bytes.toBytes(eventId)).setMaxVersions(1).addFamily(Bytes.toBytes("pi")))
      if (mergeTargetRes.isEmpty)
        throw new IllegalStateException("Oops. Merge Target")
      val mergeTargetProcessId = mergeTargetRes.getValue(Bytes.toBytes("pi"), Bytes.toBytes("process-id"))
      mergeTargetProcessId
    }


    def createProcessIndex(ev: Event, processRowKey: Array[Byte]) = {
      val newIndexPut = new Put(Bytes.toBytes(ev.id)).addColumn(Bytes.toBytes("pi"), Bytes.toBytes("process-id"), processRowKey)
      processSecondaryIndexTable.put(newIndexPut)
      debug("Created process index for event " + ev.id + " pointing to process " + Bytes.toString(processRowKey))
    }

    def updateProcessIndex(eventId: Int, processId: Array[Byte]) = {
      val newIndexPut = new Put(Bytes.toBytes(eventId)).addColumn(Bytes.toBytes("pi"), Bytes.toBytes("process-id"), processId)
      processSecondaryIndexTable.put(newIndexPut)
      debug("Updated process index for event " + eventId+ " pointing to process " + Bytes.toString(processId))
    }

    def createProcess(ev: Event, rowKey: Array[Byte]) = {
      processesTable.put(new Put(rowKey).addColumn(Bytes.toBytes("ps"), Bytes.toBytes(ev.id), Bytes.toBytes(ev.id)))
      debug("Created/Updated process " + Bytes.toInt(rowKey) + " pointing to event " + ev.id)
    }

    def createProcessAndIndex(ev: Event, processRowKey: Array[Byte]) = {
      createProcess(ev, processRowKey)
      createProcessIndex(ev, processRowKey)
    }

    def putEvent(ev: Event) = {
     putEventFlat(ev.id, ev.predecessorId)
    }

    def putEventFlat(eventId: Int, predecessorId: Int) = {
      val eventPut = new Put(Bytes.toBytes(eventId))
      if (predecessorId > -1)
        eventPut.addColumn(Bytes.toBytes("ev"), Bytes.toBytes("predecessor-id"), Bytes.toBytes(predecessorId))
      eventPut.addColumn(Bytes.toBytes("ev"), Bytes.toBytes("text"), Bytes.toBytes("llll"))
      eventsTable.put(eventPut)
    }

    def processNewEvent(ev: Event) = {

      var checkForOrphan = true

      debug("processNewEvent for event " + ev.id + ": " + ev)

      // insert event into events
      putEvent(ev)

      // event is root
      if (ev.predecessorId > -1) {
        val indexLookupRes = processSecondaryIndexTable.get(new Get(Bytes.toBytes(ev.predecessorId)).setMaxVersions(1).addFamily(Bytes.toBytes("pi")))


        if (!indexLookupRes.isEmpty) {
          // Found index for predecessor
          // Find process and maintainProcessReference

          val processIdKey = indexLookupRes.getValue(Bytes.toBytes("pi"), Bytes.toBytes("process-id"))

          val process = processesTable.get(new Get(processIdKey).addFamily(Bytes.toBytes("ps")))

          if (!process.isEmpty) {
            // process found, maintain reference to this event then
            val rowKey = process.getRow

            println("==========")

            val qvs = process.getFamilyMap(_b("ps")).asScala.foreach {
              case (q, v) => println("q=" + _i(q) + " v=" + _i(v))
            }

            createProcessAndIndex(ev, rowKey)
            // first ev.id could be step or system, if unique for one process
          } else {
            // now write a new process, pointing to this event
            debug("Write new process, pointing to this event:")
            val processRowKey = Bytes.toBytes(createUniqueId)
            createProcessAndIndex(ev, processRowKey)
          }

        } else {
          println("Haven't found index for predecessor " + ev.predecessorId + " Predecessor not created yet?")
          // FIXME Deal with this situation in the cleanup
          // FIXME Either by checking for events that have a predecssor Id, but not according entry in the secondary index
          // FIXME Or/And during event creation scanning(?) for a dependent event that has been created before

          // Capture orphaned event
          val orphanPut = new Put(Bytes.toBytes(ev.predecessorId))
          orphanPut.addColumn(Bytes.toBytes("oe"), Bytes.toBytes("orphaned-id"), Bytes.toBytes(ev.id))
          orphanedEventsTable.put(orphanPut)

          // Create index and Process

          val rowKey = Bytes.toBytes(createUniqueId)
          createProcessAndIndex(ev, rowKey)

          checkForOrphan = false

        }
      } else {
        // FIXME Deal with an unspecified predecessor, otherwise called root
        val rowKey = Bytes.toBytes(createUniqueId)
        createProcessAndIndex(ev, rowKey)

      }

      // ----

      // Now check if this event resolves an orphan?

      if (checkForOrphan)
        reconcile(processesTable, orphanedEventsTable, ev)

        // Find process and maintainProcessReference
        // FIXME Has this not been implemented yet?

        // ---
    }


    val started = System.currentTimeMillis()

    //eventsTable.getScanner(Scan)

    events.foreach(processNewEvent)

    // FIXME Write reconcilation code
    // Deal with two processes that belong together

    println("Before close " + (System.currentTimeMillis() - started))

    closeTable(eventsTable)
    closeTable(processesTable)
    closeTable(processSecondaryIndexTable)
    closeTable(orphanedEventsTable)

    println("After close "+(System.currentTimeMillis()-started))
  }


}
