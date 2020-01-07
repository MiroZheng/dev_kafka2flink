package com.miluo.flink.util

import java.io.{FileInputStream, BufferedInputStream}
import java.util.Properties

/**
  * Created by root on 2018/12/25.
  */
object Config {
  val props =new Properties()

  val filePath: String = System.getProperty("user.dir")+"/resources/config.properties"
  println("配置文件位置："+filePath)

  val in: BufferedInputStream = new BufferedInputStream(new FileInputStream(filePath))

  props.load(in)

  def main(args: Array[String]) {
    val props1: Properties = Config.props
    println(props1.getProperty("ZKQUORUM"))
  }

}
