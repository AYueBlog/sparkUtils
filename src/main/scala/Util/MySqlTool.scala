package Util

import java.sql.{DriverManager, ResultSet, Statement}
import java.util

import scala.collection.mutable.ListBuffer

/** ************************************
  * Copyright (C), Navinfo
  * Package: Util
  * Author: wulongyue06158
  * Date: Created in 2018/12/4 16:24
  * *************************************/
object MySqlTool {
  val dbc = "jdbc:mysql://datalake-data01:3306/datalake_backup"
  Class.forName("com.mysql.jdbc.Driver").newInstance()
  val conn = DriverManager.getConnection(dbc, "root", "root")
  val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

  //第一个任务初始化mysql对应信息
  def result1(pid:String,total:Long): Unit ={
    val prep = conn.prepareStatement(s"INSERT INTO calc VALUES (${pid},${total},0,0,0,0) ",Statement.RETURN_GENERATED_KEYS)
    prep.executeUpdate
  }

  //更新进度
  def updata(pid:String,result:String,total:Long): Unit ={
    val prep = conn.prepareStatement(s"update calc set ${result}=${total} where pid=${pid}")
    prep.executeUpdate
  }

  //查找本次需要计算的轨迹
  def queryPid(lastResult:String,currentResult:String): List[String] ={
    val sql=s"select pid from calc where ${currentResult}=0 and ${lastResult}!=0"
    println(sql)
    val prepquery = conn.prepareStatement(sql)
    val resultSet = prepquery.executeQuery()
    val pids = new ListBuffer[String]
    while (resultSet.next()) {
      val pid = resultSet.getString("pid")
      pids.append(pid)
//      pids:+pid
//      pids+:pid
    }
    pids.toList
  }

}
