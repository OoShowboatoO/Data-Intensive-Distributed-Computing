package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class A6Q7Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "shipped date", required = true)
    val text = opt[Boolean](descr = "text", required = false)
    val parquet = opt[Boolean](descr = "parquet", required = false)
    verify()
}

object Q7 {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new A6Q7Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Shipped Date: " + args.date())
        log.info("Text: " + args.text())
        log.info("Parquet: " + args.parquet())

        val conf = new SparkConf().setAppName("SQL Data Analytics Q7")
        val sc = new SparkContext(conf)

        val targetDate =  args.date()

        if (args.text()) {
            val lineitemPath = args.input() + "/lineitem.tbl"
            val ordersPath = args.input() + "/orders.tbl"
            val customerPath = args.input() + "/customer.tbl"

            val lineitem = sc.textFile(lineitemPath)
                .filter(line =>  {
                    val data = line.split("\\|")
                    val shipdate = data(10) 
                    shipdate > targetDate
                })
                .map(line =>{
                    val data = line.split("\\|")
                    val orderkey = data(0)
                    val extendedprice = data(5).toDouble
                    val discount = data(6).toDouble
                    val singleRev = extendedprice*( 1 - discount)
                    val shipdate = data(10)
                    (orderkey, (singleRev, shipdate))
                })

            val customer = sc.textFile(customerPath)
                .map(line => {
                    val data = line.split("\\|")
                    // (custkey, cname)
                    (data(0), data(1))
                })
                .collectAsMap()
            val bcCustomer = sc.broadcast(customer)

            val orders = sc.textFile(ordersPath)
                .filter(line => {
                    val data = line.split("\\|")
                    val custkey = data(1)
                    val orderdate = data(4) 
                    orderdate < targetDate && bcCustomer.value.contains(custkey)
                })
                .map(line => {
                    val data = line.split("\\|")
                    val orderkey = data(0)
                    val custkey = data(1)
                    val cname = bcCustomer.value(custkey)
                    val orderdate = data(4)
                    val shippriority = data(7)
                    (orderkey, (cname, orderdate, shippriority))
                })

            val outcome = lineitem.cogroup(orders) // (orderkey, (Iterator(revenue, shipdate), Iterator(cname, orderdate, shippriority)))
                .filter(line =>  line._2._1.size != 0 && line._2._2.size != 0)
                .flatMap(line => {
                    val orderkey = line._1
                    val cname = line._2._2.head._1
                    val orderdate = line._2._2.head._2
                    val shippriority = line._2._2.head._3
                    // ((cname, orderkey, orderdate, shippriority), revenue)
                    for (revDatePair <- line._2._1) yield ((cname, orderkey, orderdate, shippriority), revDatePair._1)
                })
                .reduceByKey(_ + _)
                .map(line => (line._2, line._1))
                .sortByKey(false)
                .take(5)
                // (cname, orderkey, revenue, orderdate, shippriority)
                .foreach(line => println(line._2._1, line._2._2, line._1, line._2._3, line._2._4))
            
        } else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate
            
            val lineitemPath = args.input() + "/lineitem"
            val lineitemDF = sparkSession.read.parquet(lineitemPath)
            val lineitemRDD = lineitemDF.rdd

            val ordersPath = args.input() + "/orders"
            val ordersDF = sparkSession.read.parquet(ordersPath)
            val ordersRDD = ordersDF.rdd

            val customerPath = args.input() + "/customer"
            val customerDF = sparkSession.read.parquet(customerPath)
            val customerRDD = customerDF.rdd
                  
            val lineitem = lineitemRDD
                .filter(line => line(10).toString > targetDate)
                .map(data =>{ 
                    val orderkey = data(0).toString
                    val extendedprice = data(5).toString.toDouble
                    val discount = data(6).toString.toDouble
                    val singleRev = extendedprice*( 1 - discount)
                    val shipdate = data(10).toString
                    (orderkey, (singleRev, shipdate))
                })

            val customer = customerRDD
                .map(data => {
                    // (custkey, cname)
                    (data(0).toString, data(1).toString)
                })
                .collectAsMap()
            val bcCustomer = sc.broadcast(customer)

            val orders = ordersRDD
                .filter(data => {
                    val custkey = data(1).toString
                    val orderdate = data(4).toString
                    orderdate < targetDate && bcCustomer.value.contains(custkey)
                })
                .map(data => {
                    val orderkey = data(0).toString
                    val custkey = data(1).toString
                    val cname = bcCustomer.value(custkey)
                    val orderdate = data(4).toString
                    val shippriority = data(7).toString
                    (orderkey, (cname, orderdate, shippriority))
                })


            val outcome = lineitem.cogroup(orders) // (orderkey, (Iterator(revenue, shipdate), Iterator(cname, orderdate, shippriority)))
                .filter(line =>  line._2._1.size != 0 && line._2._2.size != 0)
                .flatMap(line => {
                    val orderkey = line._1
                    val cname = line._2._2.head._1
                    val orderdate = line._2._2.head._2
                    val shippriority = line._2._2.head._3
                    // ((cname, orderkey, orderdate, shippriority), revenue)
                    for (revDatePair <- line._2._1) yield ((cname, orderkey, orderdate, shippriority), revDatePair._1)
                })
                .reduceByKey(_ + _)
                .map(line => (line._2, line._1))
                .sortByKey(false)
                .take(5)
                // (cname, orderkey, revenue, orderdate, shippriority)
                .foreach(line => println(line._2._1, line._2._2, line._1, line._2._3, line._2._4))
        }
    }
}