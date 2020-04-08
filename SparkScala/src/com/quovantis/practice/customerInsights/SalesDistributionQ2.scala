package com.quovantis.practice.customerInsights

import org.apache.log4j._
import org.apache.spark.SparkContext

object SalesDistributionQ2 {

  def getProductObject(line: String) = {
    val fields = line.split("\\|");
    val prodId = fields(0)
    val prodName = fields(1)
    val prodType = fields(2)
    (prodId, (prodName, prodType))
  };

  def getSalesObject(line: String) = {
    val fields = line.split("\\|");
    val prodId = fields(2)
    val amt = fields(4).replace("$", "").toDouble
    val qntity = fields(5).toInt
    (prodId, (amt, qntity))
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SalesDistributionQ2")

    val productLines = sc.textFile("/home/ayush/Documents/Scala/SparkScala/data/Product.txt")

    val productRdd = productLines.map(getProductObject).reduceByKey((x, y) => (x._1 + y._1, x._1 + y._1)).sortByKey().collect()

    val salesLines = sc.textFile("/home/ayush/Documents/Scala/SparkScala/data/Sales.txt")

    val salesRdd = salesLines.map(getSalesObject).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).collect().toMap

    productRdd.map(prod => {
      if (salesRdd.contains(prod._1)) {
      val sales = salesRdd.get(prod._1)
      println("Product id: " + prod._1 + " Product Name: " + prod._2._1 + " Product Type: " + prod._2._2 + " Quantity: " + sales.get._2 + " Total Amt: " + sales.get._1)
      }
    })
  }
}