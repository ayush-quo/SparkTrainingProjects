package com.quovantis.practice.trafficSource.dataframe

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object TrafficDistribution {
  def getTrafficObject(line: String) = {
    // userId, url, date 
    val fields = line.split("\\s+");
    val custId = fields(0)
    val url = fields(1).trim()
    val date = fields(2)
    (custId, url, date)
  }
  

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("TrafficDistribution").master("local[*]").getOrCreate()

    import spark.implicits._

    val trafficLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/TRAFFIC_SOURCE.txt")

    val trafficData = trafficLines.map(getTrafficObject).toDF("cust_id", "url", "date")
    val filteredData = trafficData.filter(trafficData("url").contains("adobe"))
    filteredData.select("cust_id", "date").show()
    spark.close()
  }
    
}