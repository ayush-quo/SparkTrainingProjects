package com.quovantis.practice.meeting.dataframe


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StudentOperations {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("StudentOperations").master("local[*]").getOrCreate()
    
    import spark.implicits._
    
    val studentDf = spark.read.format("csv").option("header", true)
    .load("/home/ayush/Documents/Scala/SparkScala/data/student_problem/student.csv")
    .withColumn("class-section", concat($"class",lit("-"), $"section"))
    
    val marksDf = spark.read.format("csv").option("header", true)
    .load("/home/ayush/Documents/Scala/SparkScala/data/student_problem/marks.csv")
    .withColumn("total_marks", $"english" + $"hindi" + $"math" + $"computers")
    .withColumn("percentage", $"total_marks"/4).orderBy(desc("percentage"))
    .withColumn("result", when($"percentage" geq(50), "Passed").otherwise("Fail"))
    
    
    val joinedDf = studentDf.join(marksDf, studentDf("admn_no") === marksDf("admn_no"), "inner").cache()
    
    
//    Solution to Question 1
    marksDf.select($"admn_no", $"total_marks").show(5000)
    
    
//    Solution of Question 2 
    val studentResultCount = joinedDf.groupBy($"class-section", $"result").count().sort($"class-section")
    studentResultCount.show(500)
    
//    Solution to Question 3 <-- Took Pranav's help to solve it. -->
    val resultListDf = joinedDf.groupBy($"class-section").agg(collect_list($"total_marks").as("marks_list"))
    .select($"class-section", explode(slice(sort_array($"marks_list", false), 1, 3)).as("marks"))
    .orderBy($"class-section", desc("marks"))
    val sortedResult = resultListDf.join(marksDf,  resultListDf("marks") === marksDf("total_marks"), "inner")
    .select($"admn_no", $"class-section",$"marks", $"percentage").sort(asc("class-section"), desc("marks"))
    
    sortedResult.show(5000)
    
// Solution of Question 4
    val highestMarks = joinedDf.groupBy($"class").agg(max("total_marks").as("highest"))
    highestMarks.join(marksDf,  highestMarks("highest") === marksDf("total_marks"), "inner")
    .select($"admn_no", $"class",$"total_marks", $"percentage").sort(asc("class")).show()    
        
    spark.close()
  }
}
