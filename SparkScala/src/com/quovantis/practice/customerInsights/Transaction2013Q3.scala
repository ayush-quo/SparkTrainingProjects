package com.quovantis.practice.customerInsights

import org.apache.spark.SparkContext
import org.apache.log4j._

object Transaction2013Q3 {
  def getRefundObject(line: String) = {
    val fields = line.split("\\|");
    val txnId = fields(1)
    txnId
  };

  def getSalesObject(line: String) = {
    val fields = line.split("\\|");
    val txnId = fields(0)
    val date = fields(3)
    val amt = fields(4).replace("$", "").toDouble
    (txnId, (amt, date))
  }
 

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Transaction2013Q3")

    val refundLines = sc.textFile("/home/ayush/Documents/Scala/SparkScala/data/Refund.txt")

    val refundRdd = refundLines.map(getRefundObject).collect()

    val salesLines = sc.textFile("/home/ayush/Documents/Scala/SparkScala/data/Sales.txt")

    val salesRdd = salesLines.map(getSalesObject).sortByKey().filter(sale => ((sale._2._2 contains "2013") &&  !refundRdd.contains(sale._1)))

    val totalAmt = salesRdd.map(x => x._2._1).reduce((x, y)=> x + y)
    println(totalAmt)
  }
}