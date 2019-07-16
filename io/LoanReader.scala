package com.hz.spark.io

import com.hz.spark.types.LoanType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._

trait LoanReader extends Logging {

  def readLoanData(inputPath: String, spark: SparkSession): Dataset[LoanType] = {

    import spark.implicits._

    val rawData = spark.read.option("header", "true").csv(inputPath)

    logInfo("reading data from %s".format(inputPath))

    val filteredRawDf = rawData
      .filter($"loan_status" =!= "Fully Paid") //not fully paid

    val fields = List("loan_amnt", "term", "int_rate", "installment", "home_ownership",
      "annual_inc", "emp_length", "title", "addr_state", "loan_status", "tot_coll_amt").map(col)

    filteredRawDf.select(fields: _*)
      .withColumn("has_collection", when($"tot_coll_amt" =!= "0", 1).otherwise(0).as("has_collection"))
      .withColumn("DTI", $"installment"/($"annual_inc"/12))
      .drop("loan_status")
      .drop("tot_coll_amt")
      .as[LoanType]
  }
}
