package com.miluo.flink.util

import java.sql.{Connection, DriverManager, PreparedStatement}


object SinkToMysql {
  def writeMysql(id : Int,name :String,password : String,age :Int): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into student(id,name,password,age) values (?,?,?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
      "root", "123456")

        ps = conn.prepareStatement(sql)
        ps.setInt(1, id)
        ps.setString(2, name)
        ps.setString(3, password)
        ps.setInt(4, age)
        ps.executeUpdate()

    } catch {
      case e: Exception => println("Mysql Exception"+ e.getMessage)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    SinkToMysql.writeMysql(1,"miro","aaa",8)
  }

}
