package com.hz.spark.types

case class LoanType (
                      loan_amnt: Option[String],
                      term: Option[String],
                      int_rate: Option[String],
                      installment: Option[String],
                      home_ownership: Option[String],
                      annual_inc: Option[String],
                      DTI: Option[String],
                      addr_state: Option[String],
                      emp_length: Option[String],
                      title: Option[String],
                      has_collection: Option[Int]
                    )
