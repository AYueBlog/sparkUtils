package accumulator

import org.apache.spark.util.AccumulatorV2

/** ************************************
  * Copyright (C), Navinfo
  * Package: accumulator
  * Author: wulongyue06158
  * Date: Created in 2018/12/5 12:40
  * *************************************/
class AddAccumulator extends AccumulatorV2{
  var temp:Int=0

//  如果此累加器为零值则返回。例如，对于计数器累加器，0为零值; 对于列表累加器，Nil是零值。
  override def isZero: Boolean = {
    temp==0
  }

//  创建此累加器的新副本
  override def copy(): AccumulatorV2[Nothing, Nothing] = {
    val accumulator = new AddAccumulator
    accumulator.temp=this.temp
    accumulator
  }

  override def reset(): Unit = {
    temp=0
  }

  override def add(v: Nothing): Unit = {
    temp+=Integer.parseInt(v)
  }

  override def merge(other: AccumulatorV2[Nothing, Nothing]): Unit = {

  }

  override def value = ???
}
