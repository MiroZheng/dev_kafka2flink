package com.miluo.flink.util

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Connection}

/**
  * Created by root on 2018/11/14.
  */
object HBaseUtil {
  private val props: Properties = Config.props
  private val zkList: String = props.getProperty("ZKQUORUM")
  private val hbasePort: String = props.getProperty("HBASEPORT")
  private val hbaseMaster: String = props.getProperty("HBASEMASTER")
  private val hbaseParent: String = props.getProperty("HBASEPARENT")
  private val hbaseRootDir: String = props.getProperty("HBASEROOTDIR")




  def getHBaseConn() :Connection ={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkList)
    conf.set("hbase.zookeeper.property.clientPort", hbasePort)
    conf.set("hbase.master", hbaseMaster)
    conf.set("zookeeper.znode.parent", hbaseParent)
    conf.set("hbase.rootdir", hbaseRootDir)
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }

}
