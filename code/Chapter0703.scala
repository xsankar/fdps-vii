import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ // for implicit conversations
import org.apache.spark.sql._

object Chapter0703 {
  // register case class external to main
  case class Order(OrderID : String, CustomerID : String, EmployeeID : String,
    OrderDate : String, ShipCountry : String)
    //
  case class OrderDetails(OrderID : String, ProductID : String, UnitPrice : Float,
    Qty : Int, Discount : Float)
    //
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Chapter 7")
    println(s"Running Spark Version ${sc.version}")
    //
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD // to implicitly convert an RDD to a SchemaRDD.
    import sqlContext._
    //
    val ordersFile = sc.textFile("/Users/ksankar/fdps-vii/NW-Orders-NoHdr.csv")
    println("Orders File has %d Lines.".format(ordersFile.count()))
    val orders = ordersFile.map(_.split(",")).
      map(e => Order( e(0), e(1), e(2),e(3), e(4) ))
     println(orders.count)
     orders.registerTempTable("Orders")
     var result = sqlContext.sql("SELECT * from Orders")
     result.foreach(println)
  }
}