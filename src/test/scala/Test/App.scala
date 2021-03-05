package a5

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object ThreeDivisibleRate {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/");

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("ThreeDivisibleRate").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val intLines = sc.textFile("inputs/ints.txt")
    val intSplit = intLines.flatMap(_.split(" ")).map(_.toInt).filter(_%3 == 0).countByValue()
      .map(x => x._1 + " appears " + x._2 + " times")
    println(intSplit.mkString(", "))
  }
}