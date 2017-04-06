package neartime

import java.security.MessageDigest

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._

import collection.JavaConverters._

trait HBaseApp {

  def closeHBaseConnection(connection: Connection) = {
    connection.close()
  }

  def closeTable(table: Table) = {
    table.close()
  }

  def setupTable(connection: Connection, tblName: String, initialization: (HTableDescriptor) => HTableDescriptor) = {
    val admin = connection.getAdmin

    val tableName = TableName.valueOf(tblName)

    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    admin.createTable(initialization(new HTableDescriptor(tableName)))

    connection.getTable(tableName)

  }

  def openHBaseConnection = {
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.master", "localhost:60001")

    ConnectionFactory.createConnection(hbaseConfig)
  }

  def createUniqueId = getMd5(System.nanoTime().toString)

  private def getMd5(inputStr: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") {_ + _}
  }
}
