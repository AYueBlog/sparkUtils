package Util

import com.navinfo.datalaker.aimap.util.TMap
import com.vividsolutions.jts.geom.Coordinate

/** ************************************
  * Copyright (C), Navinfo
  * Package: Util
  * Author: wulongyue06158
  * Date: Created in 2018/12/4 17:48
  * *************************************/
object TestMysql {

  def getJL(): Unit = {
    val lastcoordinate = new Coordinate(40.13158210829374,116.67667397337749)
    val curcoordinate = new Coordinate(40.13157958487577,116.67666298800405)
    //    val curcoordinate = new Coordinate(40.13156192095,116.67658609039)
    val distance = TMap.getDistance(curcoordinate, lastcoordinate)
    println(distance)
  }

  def test1(): Unit = {
    import scala.math._
    println(sqrt(2))
    println(pow(2,4))
  }

  def test2(): Unit = {
    println("hello" (4))
  }

  def test3(): Unit = {
    println("hello".take(2))
    val hello = "hello"
    println(hello.drop(3))
  }

  def main(args: Array[String]): Unit = {

//    getJL()

//    test1()

//    test2()

    test3()

  }



}
