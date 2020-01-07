package com.miluo.flink

import java.text.SimpleDateFormat
import java.util.Date

import com.miluo.flink.util.HBaseUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Put, Table, Admin, Connection}

/**
  * Created by root on 2018/12/25.
  */
object Test {
  def main(args: Array[String]) {
    val conn: Connection = HBaseUtil.getHBaseConn()
    val admin: Admin = conn.getAdmin
    val tn: TableName = TableName.valueOf("flink2hbase")

    if (!admin.tableExists(tn)) {
      admin.createTable(new HTableDescriptor(tn).addFamily(new HColumnDescriptor("info").setBlockCacheEnabled(true)))
    }
    val table: Table = conn.getTable(tn)
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    val put: Put = new Put(Bytes.toBytes(df.format(new Date())))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("miluo"))
    println("########### start--put ###########")
    table.put(put)
    println("########### end--put ###########")
    table.close()
  }


}
