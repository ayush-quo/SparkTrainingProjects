package com.quovantis.practice.customerInsights.dataframe

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SalesDistribution {
  
  def getProductObject(line: String) = {
    // product_id, product_name, product_type, product_version, product_price
    val fields = line.split("\\|");
    val prodId = fields(0)
    val prodName = fields(1)
    val prodType = fields(2)
    (prodId, prodName, prodType)
  }

  def getSalesObject(line: String) = {
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity 
    val fields = line.split("\\|");
    val prodId = fields(2)
    val amt = fields(4).replace("$", "").toDouble
    val qntity = fields(5).toInt
    (prodId, amt, qntity)
  }
  
   def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SalesDistribution")
      .master("local[*]").getOrCreate()
      
      import spark.implicits._
      
      val productLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Product.txt")
      val salesLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Sales.txt")
      
      val productData = productLines.map(getProductObject).toDF("product_id", "prod_name", "prod_type")
      val salesData = salesLines.map(getSalesObject).toDF("prod_id", "amount", "quantity")
      val groupedSalesData = salesData.groupBy("prod_id").agg(sum("amount").as("Total Amount"), sum("quantity").as("Total Quantity"))
      .orderBy("prod_id")
      val resultset = groupedSalesData.join(productData, groupedSalesData("prod_id") === productData("product_id"))
      .select("product_id", "prod_name", "prod_type", "Total Amount", "Total Quantity")
      resultset.orderBy("product_id").show(23, false)
      spark.close()
      
   }
}