package com.quovantis.practice.customerInsights

import org.apache.spark.SparkContext
import org.apache.log4j._

object UserSameProductQ6 {
  def getSalesObject(line: String) = {
    val fields = line.split("\\|");
    val custId = fields(1)
     val prodId = fields(2)
    val date = fields(3).split(" ")(0)
    (prodId + "-" + date + "-" + custId)
  }
 
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "UserSameProductQ6")

    val salesLines = sc.textFile("/home/ayush/Documents/Scala/SparkScala/data/Sales.txt")

    val salesRdd = salesLines.map(getSalesObject).map(x => (x,1)).reduceByKey((x,y) => x+y)
    

    val filteredRdd = salesRdd.filter(sale => (sale._2 > 1)).count()
    println(filteredRdd)
  }
}