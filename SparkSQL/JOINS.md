val emp1 = sc.parallelize(List(
  (10, "Inventory", "Hybd"),
  (20, "Finance", "Bglr"),
  (30, "HR", "Delhi"),
  (40, "IT", "Mumbai"),
  (50, "Marketing", "Chennai")
)).toDF("DeptNo", "Department", "Location")

emp1.show()

import spark.implicits._

val emp2 = sc.parallelize(List(
  (111, "Saketh", "Analyst", 444, 10),
  (112, "Alice", "Manager", 445, 20),
  (113, "Bob", "Clerk", 446, 10),
  (114, "Eve", "Analyst", 447, 30),
  (115, "Charlie", "Manager", 448, 40),
  (116, "David", "Clerk", 449, 20),
  (117, "Fiona", "Analyst", 450, 10)
)).toDF("Empno", "Ename", "Job", "Mgr", "DeptNo")

emp2.show()


### **1️⃣Join**

```scala
val joinedDF = emp1.join(emp2, Seq("DeptNo"), "inner")
joinedDF.show()
```


### **1️⃣ Right Outer**

```scala
val rightJoinDF = emp1.join(emp2, Seq("DeptNo"), "right_outer")
rightJoinDF.show()
```

### **1️⃣ Left Outer Join**

```scala
val leftJoinDF = emp1.join(emp2, Seq("DeptNo"), "left_outer")
leftJoinDF.show()
```
### **1️⃣ Full Outer Join**

```scala
val fullOuterDF = emp1.join(emp2, Seq("DeptNo"), "full_outer")
fullOuterDF.show()
```

**Explanation:**

* Returns **all rows** from both `emp1` and `emp2`.
* Columns from missing sides will be `null`.

---

### **2️⃣ Cross Join (Cartesian Product)**

```scala
val crossJoinDF = emp1.crossJoin(emp2)
crossJoinDF.show()
```

**Explanation:**

* Every row in `emp1` is paired with every row in `emp2`.
* Be careful with large datasets—this can explode in size.

---

### **3️⃣ Semi Join**

```scala
val semiJoinDF = emp1.join(emp2, Seq("DeptNo"), "left_semi")
semiJoinDF.show()
```

**Explanation:**

* Returns **only rows from `emp1`** that have a match in `emp2`.
* No columns from `emp2` are included.

---

### **4️⃣ Anti Join**

```scala
val antiJoinDF = emp1.join(emp2, Seq("DeptNo"), "left_anti")
antiJoinDF.show()
```

**Explanation:**

* Returns **rows from `emp1`** that do **not** have a match in `emp2`.

---

### **5️⃣ Filter Example**

```scala
val highEmpnoDF = emp2.filter($"Empno" > 113)
highEmpnoDF.show()
```

**Explanation:**

* Filters employees with `Empno > 113`.

---

### **6️⃣ Select Specific Columns**

```scala
emp2.select("Ename", "Job", "DeptNo").show()
```

**Explanation:**

* Only selects the chosen columns from `emp2`.

---

### **7️⃣ GroupBy and Aggregate**

```scala
emp2.groupBy("DeptNo")
    .agg(count("*").alias("NumEmployees"), avg("Empno").alias("AvgEmpno"))
    .show()
```

**Explanation:**

* Groups employees by `DeptNo` and shows **number of employees** and **average Empno** in each department.

---

### **8️⃣ Order By**

```scala
emp2.orderBy($"Empno".desc).show()
```

**Explanation:**

* Sorts employees by `Empno` in **descending order**.

---

### **9️⃣ Distinct**

```scala
emp2.select("DeptNo").distinct().show()
```

**Explanation:**

* Shows **unique DeptNo values** from `emp2`.

---

### **10️⃣ Union Example**

```scala
val emp3 = sc.parallelize(List(
  (118, "George", "Analyst", 451, 30)
)).toDF("Empno", "Ename", "Job", "Mgr", "DeptNo")

val unionDF = emp2.union(emp3)
unionDF.show()
```



