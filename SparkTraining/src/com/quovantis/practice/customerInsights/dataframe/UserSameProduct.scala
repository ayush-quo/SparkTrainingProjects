package com.quovantis.practice.customerInsights.dataframe

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object UserSameProduct {
  
  def getSalesObject(line: String) = {
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity 
    val fields = line.split("\\|");
    val custId = fields(1)
    val prodId = fields(2)
    val date = fields(3).split(" ")(0)
    (custId, prodId, date)
  }
  

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("PerUserCountWithSameProductOnSameDay").master("local[*]").getOrCreate()

    import spark.implicits._

    val salesLine = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Sales.txt")

    val salesData = salesLine.map(getSalesObject).toDF("cust_id", "prod_id", "date")
    
    val numOfProds = salesData.groupBy("cust_id", "prod_id", "date").agg(count("*").as("count")).filter($"count" > 1).count()
    println(numOfProds)
    spark.close()

  }
 }