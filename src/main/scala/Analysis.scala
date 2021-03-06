import org.apache.spark.rdd.RDD

class Analysis {

   // data format
   // time,open,high,low,close,volume
   def calcMovingAverage(data : RDD[String], length : Int): Unit = {

   }

   def calcMACD(data : RDD[String]): Unit = {

   }

   def findLevels(data : RDD[String]) : Unit = {

   }

   def checkMACrossOver(MA : RDD[String], MA2 : RDD[String]) : Unit = {

   }

   def checkMACDCrossOver(MA : RDD[String], MA2 : RDD[String]) : Unit = {

   }

}
