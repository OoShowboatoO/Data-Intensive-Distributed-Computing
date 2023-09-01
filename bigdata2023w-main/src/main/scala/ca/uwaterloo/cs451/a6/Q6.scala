package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class A6Q6Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "shipped date", required = true)
    val text = opt[Boolean](descr = "text", required = false)
    val parquet = opt[Boolean](descr = "parquet", required = false)
    verify()
}

object Q6 {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new A6Q4Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Shipped Date: " + args.date())
        log.info("Text: " + args.text())
        log.info("Parquet: " + args.parquet())

        val conf = new SparkConf().setAppName("SQL Data Analytics Q6")
        val sc = new SparkContext(conf)

        val targetDate =  args.date()

        if (args.text()) {
            val lineitemPath = args.input() + "/lineitem.tbl"

            // find lineitems contain the target date
            val lineitem = sc.textFile(lineitemPath)
                .filter(line => {
                    val lineitemData = line.split("\\|")
                    lineitemData(10).contains(targetDate)
                })
                .map(line =>{
                    val data = line.split("\\|")
                    // (returnflag, linestatus, quantity, extendedprice, discount, tax)
                    (data(8), data(9), data(4), data(5), data(6), data(7))
                })

            // outcome = (returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
            val outcome = lineitem.map(line => {
                val returnflag = line._1.toString
                val linestatus = line._2.toString
                val quantity = line._3.toDouble
                val extendedprice = line._4.toDouble // base_price
                val discount = line._5.toDouble
                val tax = line._6.toDouble
                val disc_price = extendedprice * (1 - discount)
                val charge = extendedprice * (1 - discount) * (1 + tax)
                val order_count = 1
                ((returnflag, linestatus), (quantity, extendedprice, discount, disc_price, charge, order_count))
            })
            .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
            .map(line => {
                val order_count = line._2._6
                val avg_qty = line._2._1 / order_count
                val avg_price = line._2._2 / order_count
                val avg_disc = line._2._3 / order_count
                // (returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
                (line._1._1, line._1._2, line._2._1, line._2._2, line._2._4, line._2._5, avg_qty, avg_price, avg_disc, order_count)
            })
            .collect()
            .foreach(println)
            
        } else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate
            
            val lineitemPath = args.input() + "/lineitem"
            val lineitemDF = sparkSession.read.parquet(lineitemPath)
            val lineitemRDD = lineitemDF.rdd
            // (returnflag, linestatus, quantity, extendedprice, discount, tax)        
            val lineitem = lineitemRDD.filter(line => line(10).toString.contains(targetDate))
            .map(line => 
                (line(8).toString, line(9).toString, line(4).toString.toDouble, line(5).toString.toDouble, line(6).toString.toDouble, line(7).toString.toDouble)
            ) 
            
            // outcome = (returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
            val outcome = lineitem.map(line => {
                val returnflag = line._1
                val linestatus = line._2
                val quantity = line._3
                val extendedprice = line._4 // base_price
                val discount = line._5
                val tax = line._6
                val disc_price = extendedprice * (1 - discount)
                val charge = extendedprice * (1 - discount) * (1 + tax)
                val order_count = 1
                ((returnflag, linestatus), (quantity, extendedprice, discount, disc_price, charge, order_count))
            })
            .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
            .map(line => {
                val order_count = line._2._6
                val avg_qty = line._2._1 / order_count
                val avg_price = line._2._2 / order_count
                val avg_disc = line._2._3 / order_count
                // (returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
                (line._1._1, line._1._2, line._2._1, line._2._2, line._2._4, line._2._5, avg_qty, avg_price, avg_disc, order_count)
            })
            .collect()
            .foreach(println)
        }
    }
}
