package com.miluo.flink

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.miluo.flink.util.{HBaseUtil, Config}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.TableEnvironment

//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client._
import org.apache.flink.api.scala._


/**
  * Created by root on 2018/12/24.
  */
object ReadingFromKafka {
  private val props: Properties = Config.props
  private val group_id: String = props.getProperty("GROUP_ID")
  private val topic: String = props.getProperty("TOPIC")
  private val brokerList: String = props.getProperty("BROKER_LIST")
  private val zkQuorum: String = props.getProperty("ZKQUORUM")


  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val TableEnv = TableEnvironment.getTableEnvironment(env)


    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", zkQuorum)
    kafkaProps.setProperty("bootstrap.servers", brokerList)
    kafkaProps.setProperty("group.id", group_id)

    val myConsumer = new FlinkKafkaConsumer08[String](topic, new SimpleStringSchema(), kafkaProps)
    val transaction = env.addSource(myConsumer)
    transaction.rebalance.map {
      value =>
        println(value)
        writeIntoHBase(value, "flink2hbase")
    }
    transaction.print()
    env.execute()


  }

  def writeIntoHBase(m: String, tableName: String): Unit = {
    val conn: Connection = HBaseUtil.getHBaseConn()
    val admin: Admin = conn.getAdmin
    val tn: TableName = TableName.valueOf(tableName)

    if (!admin.tableExists(tn)) {
      admin.createTable(new HTableDescriptor(tn).addFamily(new HColumnDescriptor("info").setBlockCacheEnabled(true)))
    }
    val table: Table = conn.getTable(tn)
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    val put: Put = new Put(Bytes.toBytes(df.format(new Date())))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(m))
    table.put(put)
  }

}
