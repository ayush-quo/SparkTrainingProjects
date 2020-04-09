package com.quovantis.practice.customerInsights.dataframe

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Transactions2013 {
  def getRefundObject(line: String) = {
    // refund_id, original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund_quantity
    val fields = line.split("\\|");
    val txnId = fields(1)
    txnId
  };

  def getSalesObject(line: String) = {
    // transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity 
    val fields = line.split("\\|");
    val txnId = fields(0)
    val date = fields(3)
    val amt = fields(4).replace("$", "").toDouble
    (txnId, amt, date)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .appName("Transaction2013")
      .master("local[*]").getOrCreate()
      
      
    import spark.implicits._
      
    val refundLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Refund.txt")
    val salesLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/Sales.txt")
   
    val refundData = refundLines.map(getRefundObject).toDF("txn_id").cache()
    val salesData = salesLines.map(getSalesObject).toDF("txn_id", "amount", "date")
    val salesData2013 = salesData.filter(salesData("date").contains("2013")).cache()
    val notRefundedSalesData = salesData2013.join(refundData, salesData2013("txn_id") === refundData("txn_id"), "left")
    .filter(refundData("txn_id") isNull)
    notRefundedSalesData.agg(sum("amount")).show(false)
    spark.close()


  }
}