package com.quovantis.practice.meeting.dataframe


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object MaxSalary {
  def getDeptObject(line: String) = {
    val fields = line.split(",");
    val deptId = fields(0)
    val area = fields(2)
    (deptId, area)

  }
 
  def getEmpObject(line: String) = {
    val fields = line.split(",");
    val deptId = fields(1)
    val salary = fields(6)
    (deptId, salary)
  }
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("MaxSalary").master("local[*]").getOrCreate()
    val area = "Palana"

    import spark.implicits._

    val deptLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/dept.csv")
    val empLines = spark.sparkContext.textFile("/home/ayush/Documents/Scala/SparkScala/data/empolyee.csv")
    
    val deptObj = deptLines.map(getDeptObject).toDF("dept_id", "area")
    val empObj = empLines.map(getEmpObject).toDF("dept_id", "salary")
    
    val finalResult = empObj.join(deptObj, empObj("dept_id") === deptObj("dept_id"), "inner")
    .filter(deptObj("area") === area).select(max("salary"))
    
    finalResult.show()
  
    spark.close()
  }
}