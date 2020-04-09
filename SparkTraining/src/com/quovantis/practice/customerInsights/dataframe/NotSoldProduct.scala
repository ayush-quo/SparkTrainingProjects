package com.quovantis.practice.customerInsights.dataframe


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NotSoldProduct {
  def getProductObject(line: String) = {
    // product_id, product_name, product_type, product_version, product_price
    val fields = line.split("\\|");
    val prodId = fields(0)
    val prodName = fields(1)
    (prodId, prodName)
  }

  def getSalesObject(line: String) = {
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity 
    val fields = line.split("\\|");
    val prodId = fields(2)
    (prodId)
  }
  
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("NotSoldProduct")
      .master("local[*]").getOrCreate()
      
      import spark.implicits._
      
      val productLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Product.txt")
      val salesLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Sales.txt")
      
      val productData = productLines.map(getProductObject).toDF("product_id", "prod_name").cache()
      val salesData = salesLines.map(getSalesObject).toDF("prod_id").distinct().cache()
      
      val resultSet = productData.join(salesData, productData("product_id") === salesData("prod_id"), "left")
      .filter(salesData("prod_id") isNull)
      
      resultSet.select("product_id", "prod_name").show()
      
      spark.close()
  }
}