package com.quovantis.practice.customerInsights.dataframe

import org.apache.log4j._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SecondMostPurchases {
  def getSalesObject(line: String) = {
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity 
    val fields = line.split("\\|");
    val txnId = fields(0)
    val date = fields(3).split(" ")(0)
    val cust_id = fields(1)
    (txnId, cust_id, date)
  }
  
  def getCustomerObject(line: String) = {
    // customer_id, customer_first_name, customer_last_name, phone_number 
    val fields = line.split("\\|");
    val custId = fields(0)
    val firstName = fields(1)
    val lastName = fields(2)
    (custId, firstName, lastName)
  }
  
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SecondMostPurchases")
      .master("local[*]").getOrCreate()
      
      import spark.implicits._
      
      val customerLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Customer.txt").cache()
      val salesLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Sales.txt").cache()
      
      val customerData = customerLines.map(getCustomerObject).toDF("custId", "firstName", "lastName")
      val salesData = salesLines.map(getSalesObject).toDF("txnId", "cust_id", "date")
      
      val filteredSales = salesData.filter(salesData("date").startsWith("04") && salesData("date").endsWith("2013"))
      .groupBy("cust_id").agg(count("*").as("records")).orderBy(desc("records"))
      
      val maxTxn = filteredSales.agg(max("records").as("max")).collect()(0)(0)
     
      val secondMaxOnwards = filteredSales.filter(filteredSales("records").notEqual(maxTxn))
      val secondMax = secondMaxOnwards.filter(secondMaxOnwards("records").equalTo(secondMaxOnwards.agg(max("records").as("max")).collect()(0)(0)))
      
      val resultSet = secondMax.join(customerData, secondMax("cust_id") === customerData("custId")).select("firstName", "lastName")
      resultSet.show()
      spark.close()
  }
}