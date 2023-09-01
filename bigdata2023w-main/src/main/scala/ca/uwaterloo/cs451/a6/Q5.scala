package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class A6Q5Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val text = opt[Boolean](descr = "text", required = false)
    val parquet = opt[Boolean](descr = "parquet", required = false)
    verify()
}

object Q5 {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new A6Q5Conf(argv)
        
        log.info("Input: " + args.input())
        log.info("Text: " + args.text())
        log.info("Parquet: " + args.parquet())

        val conf = new SparkConf().setAppName("SQL Data Analytics Q5")
        val sc = new SparkContext(conf)


        if (args.text()) {
            val lineitemPath = args.input() + "/lineitem.tbl"
            val ordersPath = args.input() + "/orders.tbl"
            val customerPath = args.input() + "/customer.tbl"
            val nationPath = args.input() + "/nation.tbl"

            // find lineitems contain the target date
            val lineitem = sc.textFile(lineitemPath)
                .map(line => {
                    val lineitemData = line.split("\\|")
                    // (l_orderkey, l_shipdate(YYYY-MM))
                    val orderkey = lineitemData(0)
                    val lastDashIndex = lineitemData(10).lastIndexOf('-')
                    val date = lineitemData(10).substring(0, lastDashIndex)
                    (orderkey, date)
                })

            val orders = sc.textFile(ordersPath)
                .map(line => {
                    val orderData = line.split("\\|")
                    // (o_orderkey, o_custkey )
                    (orderData(0), orderData(1))
                })

            val customer = sc.textFile(customerPath)
                .map(line => {
                    val customerData = line.split("\\|")
                    // (c_custkey, c_nationkey)
                    (customerData(0), customerData(3))
                })
                .filter(pair => pair._2 == "3" || pair._2 == "24")
                .collectAsMap()

            val nation = sc.textFile(nationPath)
                .map(line => {
                    val nationData = line.split("\\|")
                    // (n_nationkey, n_name)
                    (nationData(0), nationData(1))
                })
                .filter(pair => pair._1 == "3" || pair._1 == "24")
                .collectAsMap()

            val bcNation = sc.broadcast(nation)
            val bcCustomer = sc.broadcast(customer)

            val orderNationPair = orders.filter(orderData => {
                bcCustomer.value.contains(orderData._2)
            })
            // => (o_orderkey, c_nationkey)
            .map(orderData => (orderData._1, bcCustomer.value(orderData._2))) 

            // outcome = (n_nationkey, n_name, count(*))
            val outcome = lineitem.cogroup(orderNationPair) // => (l_orderkey, (List l_shipdate, List c_nationkey))
                .filter(data => data._2._1.size != 0 && data._2._2.size != 0)
                .flatMap(line => {for (shipedDate <- line._2._1) yield (line._1, (shipedDate, line._2._2))})
                .filter(line => bcNation.value.contains(line._2._2.head)) // make sure the corresponding n_name existing
                .map(line => ((line._2._1, line._2._2.head, bcNation.value(line._2._2.head)), 1))
                .reduceByKey(_ + _)
                // (nationkey, n_name, date, count)
                .map(line => (line._1._2.toLong, line._1._3, line._1._1, line._2))
                .sortBy(r => (r._1, r._3))
                .collect()
                .foreach(println)
                // .foreach(p => println(p._1.toString + "," + p._2.toString + "," + p._3.toString + "," + p._4.toString))
            
        } else if (args.parquet()) {
            val sparkSession = SparkSession.builder.getOrCreate
            
            val lineitemPath = args.input() + "/lineitem"
            val lineitemDF = sparkSession.read.parquet(lineitemPath)
            val lineitemRDD = lineitemDF.rdd
            // (l_orderkey, l_shipdate)
            val lineitem = lineitemRDD.map(line => {
                val date = line(10).toString
                val lastDashIndex = date.lastIndexOf('-')
                val updatedDate = date.substring(0, lastDashIndex)
                (line(0).toString, updatedDate)
            })

            val ordersPath = args.input() + "/orders"
            val ordersDF = sparkSession.read.parquet(ordersPath)
            val ordersRDD = ordersDF.rdd
            // (o_orderkey, o_custkey )
            val orders = ordersRDD.map(line => (line(0).toString, line(1).toString))

            val customerPath = args.input() + "/customer"
            val customerDF = sparkSession.read.parquet(customerPath)
            val customerRDD = customerDF.rdd
            // (s_suppkey, s_name)
            val customer = customerRDD.map(line => (line(0).toString, line(3).toString))
            .filter(pair => pair._2 == "3" || pair._2 == "24")
            .collectAsMap()
            val bcCustomer = sc.broadcast(customer)

            val nationPath = args.input() + "/nation"
            val nationDF = sparkSession.read.parquet(nationPath)
            val nationRDD = nationDF.rdd
            // (n_nationkey, n_name)
            val nation = nationRDD.map(line => (line(0).toString, line(1).toString))
            .filter(pair => pair._1 == "3" || pair._1 == "24")
            .collectAsMap()
            val bcNation = sc.broadcast(nation)

            val orderNationPair = orders.filter(orderData => {
                bcCustomer.value.contains(orderData._2)
            })
            // => (o_orderkey, c_nationkey)
            .map(orderData => (orderData._1, bcCustomer.value(orderData._2)))  
            
            // outcome = (n_nationkey, n_name, count(*))
            val outcome = lineitem.cogroup(orderNationPair) // => (l_orderkey, (List l_shipdate, List c_nationkey))
                .filter(data => data._2._1.size != 0 && data._2._2.size != 0)
                .flatMap(line => {for (shipedDate <- line._2._1) yield (line._1, (shipedDate, line._2._2))})
                .filter(line => bcNation.value.contains(line._2._2.head)) // make sure the corresponding n_name existing
                .map(line => ((line._2._1, line._2._2.head, bcNation.value(line._2._2.head)), 1))
                .reduceByKey(_ + _)
                // (nationkey, n_name, date, count)
                .map(line => (line._1._2.toLong, line._1._3, line._1._1, line._2))
                .sortBy(r => (r._1, r._3))
                .collect()
                .foreach(println)
        }
    }
}