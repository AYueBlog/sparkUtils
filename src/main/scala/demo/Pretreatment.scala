package demo

import GeoUtil.{AdasGpsModel, TraPrepro}
import Util.MySqlTool
import com.navinfo.datalaker.aimap.util.TMap
import com.vividsolutions.jts.geom.Coordinate
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import parquet.org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/** ************************************
  * Copyright (C), Navinfo
  * Package: demo
  * Author: wulongyue06158
  * Date: Created in 2018/12/4 11:59
  * *************************************/
object Pretreatment {

  private val logger: Logger = LoggerFactory.getLogger(Pretreatment.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("encryption").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
    val sqlContext = sparkSession.sqlContext

    //加载原始文件源
    val sourceRDD = sparkContext.textFile("file:///F:/data/20180711京KH7537_1.PosD")
    //设置累加器，记录处理了多少条数据
    val encryptionAccumulator = sparkContext.longAccumulator("encryption")

    //将头信息过滤,将每行数据转换成字符串数组
    val arrayRDD = sourceRDD.map { row => {
      row.split("\\s+")
    }
    }.filter { array => {
      if (array.length == 12) {
        encryptionAccumulator.add(1L)
        true
      } else {
        false
      }
    }
    }

    //        SeqNum        GPSTime      Northing       Easting        H-Ell        Latitude       Longitude      Roll     Pitch   Heading Project Name             Q
    //        (sec)           (m)           (m)          (m)           (deg)           (deg)     (deg)     (deg)     (deg)
    //        1            4374.519  4447627.5155   728125.9026       22.199  40.13205470160 116.67645226874  -0.20940  -0.00660  91.94148 20180711京KH7537         2
    //        2            4375.706  4447627.4568   728126.9016       22.190  40.13205390251 116.67646395602  -0.20264   0.01215  95.81551 20180711¾©KH7537         2

    val tupleRDD = arrayRDD.map(arr => ("pid", arr(0).toInt, arr(1).toDouble, arr(2).toDouble, arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, arr(6).toDouble, arr(7).toDouble, arr(8).toDouble, arr(9).toDouble, arr(10), arr(11).toDouble))

    //3.4 中高精度轨迹分段
    /** *
      * 在采集过程中，设备并不是从始至终开机运行的，中间可能出现作业员吃饭、断电以及人为开关设备等状况造成同一设备一天之内采集多段轨迹，设置中途切换
      * 采集人的的场景，到预处理后，预处理依据相邻点的距离间隔进行区分。若相邻两点距离超过20米则认为该位置不连续。
      */

    // 根据pid分组 结果数据结构是 (pid，iterable(字段1，字段2....))
    val pidIterRDD = tupleRDD.groupBy(_._1)

    // 每个轨迹都有可能被处理后分成多段轨迹，返回list，因此用flatMap。 返回结果的数据结构(pid,字段1，字段2....)
    val pidSourceRDD = pidIterRDD.flatMap { case (pid, iter) => {
      //从iter(容器里面是pid对应的所有点信息) 取出除了第一个点外的所有点放到新的容器中
      val tail = iter.toList.tail
      //在新的容器尾部重复加上最后一个点的信息，为了保存最后一个点的信息
      val list2 = tail ::: List(tail.last)
      //将原来的容器和新容器通过拉链操作关联到一起，前后连个点依次关联到一起。用两个点的经纬度信息得到两点的距离可以判断出前后两点之间的距离是否查过了20m
      /**
        * 例如：list1                (1,2,3,4,5)
        * newList              (2,3,4,5,5)
        * list1.zip(newList)   ((1,2),(2,3),(3,4),(4,5),(5,5))
        */
      val brotherIter = iter.zip(list2)
      var pidSuffix = 0
      //处理每一对前后两个点，如果超过20mi就更改pid并返回。
      val pidTupleList = brotherIter.toList.map { case (last, cur) => {
        val lastcoordinate = new Coordinate((last._7), (last._8))
        val curcoordinate = new Coordinate((cur._7), (cur._8))
        val distance = TMap.getDistance(curcoordinate, lastcoordinate)
        if (distance > 20) {
          pidSuffix += 1
        }
        (pid + pidSuffix, last._2, last._3, last._4, last._5, last._6, last._7, last._8, last._9, last._10, last._11, last._12, last._13)
      }
      }
      pidTupleList
    }
    }

    //缓存一下，切断血缘关系。
    pidSourceRDD.cache()

    //3.5	过短轨迹段检测
    /**
      * 轨迹分段之后若出现少于50个点（大约5秒钟）的轨迹段则认为是采集错误，该段轨迹不使用（不影响该轨迹的其他轨迹段），
      * 并记录警告信息（错误类型为4，错误描述标识起止周秒）
      */
    val newPidSourceRDD = pidSourceRDD.groupBy(_._1)
    val filterTooShortPidSourceRDD = newPidSourceRDD.filter { case (pid, source) => {
      val countPoint = source.size
      if (countPoint < 50) {
        val head = source.head
        val last = source.last
        logger.error(pid + "轨迹少于50个点:错误类型为4" + "开始时间：" + head._2 + " 结束时间：" + last._2)
        false
      }
      true
    }
    }


    //3.6	剔除已经处理过的轨迹
    /**
      * 通过工程名判断轨迹是否已经被处理过，若被处理过则不再重复处理。
      */
    val filterProcessedPidSourceRDD = filterTooShortPidSourceRDD.filter(_ => true)


    //3.7	轨迹重复检测
    /**
      * 外业轨迹可能出现两个pad同连一个设备的情况，导致ADAS轨迹重复，这对后面将是灾难性的后果，故此在这里检测并剔除这种数据，
      * 同时记录错误信息（错误类型为8，错误描述为‘轨迹重复:’+采集人）。报出来的信息由工程项目部将出现该错误的人员信息导出通知给外业负责人进行通告规范作业
      */
    val filterRepeatPidSourceRDD = filterProcessedPidSourceRDD.filter(_ => true)


    //3.8	等距内插
    /**
      * 轨迹点是等时间间隔的，处理及之后的产品要求是等距离间隔的，所以对轨迹进行等距内插（阀值1米）
      */
    val arithmeticSequenceSourceRDD = filterRepeatPidSourceRDD.flatMap { case (pid, itera) => {
      import scala.collection.JavaConverters._
      val adasGpsModels = itera.map { source => {
        val model = new AdasGpsModel()
        model.setLinkPid(source._1)
        model.setSeqNum(source._2)
        model.setGpsTime(source._3)
        model.setNorthing(source._4)
        model.setEasting(source._5)
        model.setH_ell(source._6)
        model.setLatitude(source._7)
        model.setLongitude(source._8)
        model.setRoll(source._9)
        model.setPitch(source._10)
        model.setHeading(source._11)
        model.setProjectname(source._12)
        model.setQ(source._13)
        model
      }
      }.toList.asJava

      val adasGpsModelsResult = TraPrepro.isometricInterpolation(adasGpsModels, 1d, 0.75, 1.35)
      val tupleSource = adasGpsModelsResult.asScala.map { case adasGpsModel => {
        (
          pid,
          adasGpsModel.getSeqNum,
          adasGpsModel.getGpsTime,
          adasGpsModel.getNorthing,
          adasGpsModel.getEasting,
          adasGpsModel.getH_ell,
          adasGpsModel.getLatitude,
          adasGpsModel.getLongitude,
          adasGpsModel.getRoll,
          adasGpsModel.getPitch,
          adasGpsModel.getHeading,
          adasGpsModel.getProjectname,
          adasGpsModel.getQ
        )
      }
      }
      tupleSource
    }
    }

    arithmeticSequenceSourceRDD.saveAsTextFile("file:///F:/data/20180711京KH7537result.PosD")


    //    val dataFrame = tupleRDD.toDF()
    //    //创建临时中间表
    //    dataFrame.createOrReplaceTempView("result1_temp")
    //    //持久化到hive表中
    //    sparkSession.sql("insert into default.result1 select * from result1_temp")
    //
    //    println(sourceRDD.count())
    //    println(encryptionAccumulator.value)
    //    //    MySqlTool.result1("20180711",encryptionAccumulator.value)
    //    MySqlTool.updata("20180711", "result2", encryptionAccumulator.value)
    sparkSession.close()
    sparkContext.stop()
  }
}
