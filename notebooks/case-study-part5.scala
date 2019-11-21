// Databricks notebook source
val getOrderAmount = (units: Int, unitPrice: Int, itemdiscount: Int) => {
  val total = (units * unitPrice)
  val discount = ((total * itemdiscount) / 100).asInstanceOf[Int]
  
  (total - discount).asInstanceOf[Int]
}

val getCustomerType = (credit: Int) => {
  if(credit < 10000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

spark.udf.register("getCustomerType", getCustomerType)
spark.udf.register("getOrderAmount", getOrderAmount)

// COMMAND ----------

val sqlStatement = 
"""SELECT o.orderid AS OrderId, o.orderdate AS OrderDate, c.customername AS CustomerName, p.title AS ProductTitle,
  c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
  getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS OrderAmount,
  p.unitprice AS UnitPrice, p.itemdiscount AS ItemDiscount,
  o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
FROM PracticeDB.Orders o
INNER JOIN PracticeDB.Customers c ON c.customerid = o.customer
INNER JOIN PracticeDB.Products p ON p.productid = o.product
WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
ORDER BY OrderAmount"""

val processedOrders = spark.sql(sqlStatement)

display(processedOrders)

// COMMAND ----------

processedOrders
  .write
  .format("com.databricks.spark.csv")
  .option("header", true)
  .option("sep",",")
  .save("/mnt/data/adf-integration/processed-orders")