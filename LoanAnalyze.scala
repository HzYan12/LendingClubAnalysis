package com.hz.spark

import com.hz.spark.aggregator.LoanInfoAggregator
import com.hz.spark.aggregator.LoanInfoAggregator
import io.{LoanAggregationWriter, LoanReader, RejectionReader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object LoanAnalyze extends Logging with LoanReader with RejectionReader with LoanInfoAggregator with LoanAggregationWriter {

  def main (args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Dude, I need exact three parameters")
    }

    val spark = SparkSession
      .builder()
      .appName("Loan-analyze")
      .getOrCreate()

    val loanInputPath = args(0)
    val rejectionInputPath = args(1)
    val outputPath = args(2)

    // read loan data
    val loanDs = readLoanData(loanInputPath, spark)

    // read rejection data
    val rejectionDs = readRejectionData(rejectionInputPath, spark)

    // aggregate two datasets
    val aggregatedDf = loanInfoAggregator(rejectionDs, loanDs, spark)

    writeLoanAggregatedData(aggregatedDf, outputPath)
  }

}
