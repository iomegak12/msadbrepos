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

val statement = """
SELECT CAST(o.orderid AS STRING) AS OrderId, o.orderdate AS OrderDate, c.customername AS CustomerName, p.title AS ProductTitle,
   c.address AS CustomerLocation, getCustomerType(c.credit) AS CustomerType,
   CAST(getOrderAmount(o.units, p.unitprice, p.itemdiscount) AS STRING) AS OrderAmount,
   CAST(p.unitprice AS STRING) AS UnitPrice, CAST(p.itemdiscount AS STRING) AS ItemDiscount,
   o.billingaddress AS BillingAddress, o.remarks AS OrderRemarks
  FROM PracticeDB.Orders o
  INNER JOIN  PracticeDB.Customers c ON c.customerid = o.customer
  INNER JOIN PracticeDB.Products p ON p.productid = o.product
  WHERE o.billingaddress IN ( 'Bangalore', 'Trivandrum', 'Hyderabad', 'Mumbai', 'Chennai', 'New Delhi')
  ORDER BY OrderAmount
"""

val processedOrders = spark.sql(statement)

processedOrders.printSchema

// COMMAND ----------

val outputLocation2 = "https://iomegadlsgen2source.dfs.core.windows.net/data/processed-orders-cdm"
val cdmModel2 = "ordersystem"
val appId2 = "028c12a2-90bc-4213-b710-695f20802d9b"
val tenantId2 = "381a10df-8e85-43db-86e1-8893b075b027"
val secret2 = "J6osZEE0S02ZnZVoRQHCFWtBR-/]vpD."

// COMMAND ----------

processedOrders
  .write
  .format("com.microsoft.cdm")
  .option("entity", "processedordersv2")
  .option("appId", appId2)
  .option("appKey", secret2)
  .option("tenantId", tenantId2)
  .option("cdmFolder", outputLocation2)
  .option("cdmModelName", cdmModel2)
  .save()

// COMMAND ----------


val orders = spark
  .read
  .format("com.microsoft.cdm")
  .option("entity", "processedordersv2")
  .option("appId", appId2)
  .option("appKey", secret2)
  .option("tenantId", tenantId2)
  .option("cdmModel", "https://iomegadlsgen2source.dfs.core.windows.net/data/processed-orders-cdm/model.json")
  .load()


// COMMAND ----------

display(orders)