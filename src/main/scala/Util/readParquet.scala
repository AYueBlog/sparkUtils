package Util

import java.sql.DriverManager

import accumulator.AddAccumulator
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

/** ************************************
  * Copyright (C), Navinfo
  * Package: Util
  * Author: wulongyue06158
  * Date: Created in 2018/12/3 17:24
  * *************************************/
case class Temp(var temp:Int){}
object ReadParquet {

  // 读取Parquet文件中的数据，创建一个DataFrame
  def readParquet(sqlContext: SQLContext, url: String): _root_.org.apache.spark.sql.DataFrame = {
    val dataFrame = sqlContext.read.parquet(url)
    dataFrame
  }

  // 读取mysql中的数据，创建一个DataFrame
  def readMysql(sqlContext: SQLContext, mysqlUrl: String, mysqlDriver: String, mysqlUser: String, mysqlPassword: String, mysqlTable: String) = {
    //    val connection =() =>{
    //      Class.forName(mysqlDriver).newInstance()
    //      DriverManager.getConnection(mysqlUrl,mysqlUser,mysqlPassword)
    //    }
    //    new JdbcRDD(
    //      sparkContext,
    //      connection,
    //      mysqlSql,
    //      r =>{
    //        val id = r.getInt(1)
    //        val location = r.getString(2)
    //        val counts = r.getInt(3)
    //        val access_date = r.getDate(4)
    //        (id,location,counts,access_date)
    //      }
    //    )
    val dataFrame = sqlContext.read.format("jdbc")
      .option("dbtable", mysqlTable)
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

    ////    // 读取Parquet文件中的数据，创建一个DataFrame
    ////    val url:String=""
    ////    val parquetDataFrame = readParquet(sqlContext,url)
    //
    //    // 读取mysql中的数据，创建一个DataFrame
    //    val mysqlUrl:String="jdbc:mysql://datalake-data01:3306/datalake_backup"
    //    val mysqlDriver:String="com.mysql.jdbc.Driver"
    //    val mysqlUser:String="root"
    //    val mysqlPassword:String="root"
    //    val mysqlTable:String="t_backup_table"
    //    val mysqlDataFrame = readMysql(sqlContext,mysqlUrl,mysqlDriver,mysqlUser,mysqlPassword,mysqlTable)
    //    mysqlDataFrame.show()

//    val lists = List(1, 2, 3, 4, 5)
//    var temp:Int=0;
//    val accumulator = sparkContext.longAccumulator("calc")
//    accumulator.reset()
//    val rdd = sparkContext.parallelize(lists)
//    rdd.aggregate()
//    lists.scanLeft()

//    valueRDD.collect.foreach(println)

    val lists = List(1,2,3,4,5)
    val list = 0::lists
    val index = list.zipWithIndex
    index.foreach(println)


//    val lists = List(1,2,3,4,5)
//    val tail = lists.tail
//    val list2 = tail:::List(tail.last)
//    val tuples = lists.zip(list2)
//    tuples.foreach(println)
//    tuples.map{case (x,y)=>{
//      val sub=y-x
//      if(y-x>1){
//        (1,x)
//      }else{
//        (2,x)
//      }
//    }}.foreach(println)

//    val rdd = sparkContext.parallelize(lists)
//
//    val resultRDD = rdd.zip(rdd)
//    resultRDD.foreach(println)

//    val kvRDD = rdd.groupByKey()
//    kvRDD.map{ case (key,iter)=>{
//      var temp:Int = 0
//      iter.map{data=>{
//        temp+=data
//        temp
//      }}
//    }}.collect.foreach(println)

  }
}
