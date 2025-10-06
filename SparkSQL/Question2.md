```markdown
# SparkSQL Exercise: Transaction Analysis

We have the following CSV data:
Transaction_ID,Product,Amount
1,Laptop,1000
2,Phone,500
3,Laptop,1200
4,TV,800
````
## 1. Load data from CSV file to Spark DataFrame

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("TransactionAnalysis")
  .master("local[*]")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

val df = spark.read.option("header","true")
                   .option("inferSchema","true")
                   .csv("C:\\Local volume\\Github\\Scala-programming\\SparkSQL\\data2.csv")

df.show()
df.printSchema()
````

**Output:**

```
+--------------+-------+------+
|Transaction_ID|Product|Amount|
+--------------+-------+------+
|             1| Laptop|  1000|
|             2|  Phone|   500|
|             3| Laptop|  1200|
|             4|     TV|   800|
+--------------+-------+------+
```

---

## 2. Find the total amount earned from Laptop sales

```scala
import org.apache.spark.sql.functions._

val totalLaptopAmount = df.filter($"Product" === "Laptop")
                          .agg(sum("Amount").alias("Total_Laptop_Amount"))

totalLaptopAmount.show()
```

**Output:**

```
+-------------------+
|Total_Laptop_Amount|
+-------------------+
|               2200|
+-------------------+
```

---

## 3. Calculate the average amount of all transactions

```scala
val avgAmount = df.agg(avg("Amount").alias("Average_Amount"))
avgAmount.show()
```

**Output:**

```
+--------------+
|Average_Amount|
+--------------+
|         875.0|
+--------------+
```

---

## 4. Count the number of transactions where the amount is greater than 1000

```scala
val highAmountCount = df.filter($"Amount" > 1000).count()
println(s"Number of transactions with Amount > 1000: $highAmountCount")
```

**Output:**

```
Number of transactions with Amount > 1000: 1
```
