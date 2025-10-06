```markdown
# SparkSQL Exercise: Employee Analysis

We have the following CSV data:
Name,Age,Department,Salary
John,30,IT,50000
Alice,35,HR,60000
Bob,40,Finance,70000
```

## 1. Load data from CSV file to Spark DataFrame

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("SparkSQLExample")
  .master("local[*]")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

val df = spark.read.option("header","true")
                   .option("inferSchema","true")
                   .csv("C:\\Local volume\\Github\\Scala-programming\\SparkSQL\\data.csv")

df.show()
df.printSchema()
```

**Output:**

```
+-----+---+----------+------+
| Name|Age|Department|Salary|
+-----+---+----------+------+
| John| 30|        IT| 50000|
|Alice| 35|        HR| 60000|
|  Bob| 40|   Finance| 70000|
+-----+---+----------+------+
```

---

## 2. Find average age of Employee

```scala
import org.apache.spark.sql.functions._

val avgAge = df.agg(avg("Age").alias("Average_Age"))
avgAge.show()
```

**Output:**

```
+-----------+
|Average_Age|
+-----------+
|       35.0|
+-----------+
```

---

## 3. Retrieve the name of the employee who belong to Finance department

```scala
val financeEmp = df.filter($"Department" === "Finance")
                   .select("Name")
financeEmp.show()
```

**Output:**

```
+----+
|Name|
+----+
| Bob|
+----+
```

---

## 4. Calculate The Total Budget of the company

```scala
val totalBudget = df.agg(sum("Salary").alias("Total_Budget"))
totalBudget.show()
```

**Output:**

```
+------------+
|Total_Budget|
+------------+
|      180000|
+------------+
```
