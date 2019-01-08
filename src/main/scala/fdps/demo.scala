package fdps

import org.apache.spark.{SparkConf, SparkContext}

object demo {
  // register case class external to main
  case class Order(OrderID : String, CustomerID : String, EmployeeID : String,
                   OrderDate : String, ShipCountry : String)
  //
  case class OrderDetails(OrderID : String, ProductID : String, UnitPrice : Float,
                          Qty : Int, Discount : Float)
  def main(args: Array[String]): Unit = {
    val filePath = "E:\\fdps-v3-master/"
    val conf = new SparkConf().setAppName("AdvUrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    println(s"Running Spark Version ${sc.version}")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._
    val ordersFile = sc.textFile(filePath + "data/NW-Orders-NoHdr.csv")
    println("Orders File has %d Lines.".format(ordersFile.count()))
    val orders = ordersFile.map(_.split(","))
      .map(e => Order(e(0),e(1),e(2),e(3),e(4)))
    println(orders.count)
    orders.toDF().registerTempTable("Orders")
    var result = sqlContext.sql("select * from Orders")
    result.take(10).foreach(println)
    val orderDetFile = sc.textFile(filePath + "data/NW-Order-Details-NoHdr.csv")
    println("Order Details File has %d Lines.".format(orderDetFile.count()))
    val orderDetails = orderDetFile.map(_.split(",")).
      map(e => OrderDetails( e(0), e(1), e(2).trim.toFloat,e(3).trim.toInt, e(4).trim.toFloat ))
    println(orderDetails.count)
    orderDetails.toDF().registerTempTable("OrderDetails")
    result = sqlContext.sql("SELECT * from OrderDetails")
    result.take(10).foreach(println)

    //
    // Now the interesting part
    //
    result = sqlContext.sql("SELECT OrderDetails.OrderID,ShipCountry,UnitPrice,Qty,Discount FROM Orders INNER JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID")
    result.take(10).foreach(println)
    result.take(10).foreach(e=>println("%s | %15s | %5.2f | %d | %5.2f |".format(e(0),e(1),e(2),e(3),e(4))))
  }
}
