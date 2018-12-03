package Util

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

/** ************************************
  * Copyright (C), Navinfo
  * Package: Util
  * Author: wulongyue06158
  * Date: Created in 2018/12/3 17:24
  * *************************************/
object ReadParquet {

  // 读取Parquet文件中的数据，创建一个DataFrame
  def readParquet(sqlContext: SQLContext, url: String): _root_.org.apache.spark.sql.DataFrame = {
    val dataFrame = sqlContext.read.parquet(url)
    dataFrame
  }

  // 读取mysql中的数据，创建一个DataFrame
  def readMysql(sqlContext: SQLContext, mysqlUrl: String, mysqlDriver: String, mysqlUser: String, mysqlPassword: String, mysqlSql: String) = {
    val dataFrame = sqlContext.read.format("jdbc")
      .option("dbtable", mysqlSql)
      .option("url", mysqlUrl)
      .option("driver", mysqlDriver)
      .option("user", mysqlUser)
      .option("password", mysqlPassword).load()
    dataFrame
  }


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("read parquet").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext

//    // 读取Parquet文件中的数据，创建一个DataFrame
//    val url:String=""
//    val parquetDataFrame = readParquet(sqlContext,url)

    // 读取mysql中的数据，创建一个DataFrame
    val mysqlUrl:String="jdbc:mysql://datalake-data01:3306/datalake_backup"
    val mysqlDriver:String="com.mysql.jdbc.Driver"
    val mysqlUser:String="root"
    val mysqlPassword:String="root"
    val mysqlSql:String="select * from t_backup_strategy"
    val mysqlDataFrame = readMysql(sqlContext,mysqlUrl,mysqlDriver,mysqlUser,mysqlPassword,mysqlSql)
    mysqlDataFrame.show()

  }
}
