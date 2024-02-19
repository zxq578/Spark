from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Employee Department Analysis") \
    .getOrCreate()

# Read CSV File
df = spark.read.csv("/Users/xueqin/Desktop/Spark/Employee_Department.csv", header=True, inferSchema=True)

# Show the first few rows of the DataFrame

''' DF and SparkSQL'''
#1. Find the total number of employees in the company
df.createOrReplaceTempView("employees")
'''total_employees_sql = spark.sql("SELECT COUNT(*) AS total_employees FROM employees")
total_employees_sql.show()


total_employees = df.count()
print(f"Total number of employees: {total_employees}")
'''
#2. Find total number of departments in the company

'''total_department_sql=spark.sql("SELECT COUNT(DISTINCT department) AS total_departments FROM employees")
total_department_sql.show()


total_departments = df.select("department").distinct().count()

print(f"Total number of unique departments: {total_departments}")'''

#3. Find the department names of the company
'''department_name_sql=spark.sql("SELECT DISTINCT department FROM employees")
department_name_sql.show()

department_name=df.select("department").distinct().show()'''
#4. Find the total number of employees in each department
from pyspark.sql import functions as F
'''emp_dep_sql=spark.sql("SELECT department, COUNT(*) AS num_of_employees FROM employees GROUP BY department")
emp_dep_sql.show()

emp_dep = df.groupBy("department").agg(F.count("*").alias("num_of_employees"))
emp_dep.show()
'''

#5. Find the total number of employees in each state

'''emp_state_sql=spark.sql("SELECT state, count(*) AS num_of_employees FROM employees GROUP BY STATE ")
emp_state_sql.show()

emp_state=df.groupBy("State").agg(F.count("*").alias("num_of_employees"))
emp_state.show()
'''
#6. Find the total number of employees in each state of each department
'''
emp_state_sql=spark.sql("SELECT state, department, count(*) AS num_of_employees FROM employees GROUP BY state, department ")
emp_state_sql.show()

emp_state=df.groupBy("State", "Department").agg(F.count("*").alias("num_of_employees"))
emp_state.show()
'''
#7. Find the min and max salaries in each department and sort salaries in ascending order
'''
emp_dep_salary_sql = spark.sql("""
SELECT department, MIN(salary) AS min_salary, MAX(salary) AS max_salary 
FROM employees 
GROUP BY department 
ORDER BY MIN(salary)
""")
emp_dep_salary_sql.show()



emp_dep_salary = df.groupBy("department") \
                   .agg(F.min("salary").alias("min_salary"),
                        F.max("salary").alias("max_salary")) \
                   .orderBy(F.col("min_salary"))

emp_dep_salary.show()
'''


'''8. Find the names of employees working in NY state under Finance department whose
bonuses are greater than the average bonuses of employees in NY state'''

# Calculate the average bonus for employees in NY state

from pyspark.sql import functions as F

# Calculate the average bonus for employees in NY state
avg_bonus_ny = df.filter(F.col("State") == "NY") \
                 .agg(F.avg("bonus").alias("avg_bonus")) \
                 .collect()[0]["avg_bonus"]




# Filter employees in the Finance department in NY state with bonuses greater than the average
employees_above_avg_bonus = df.filter(
    (F.col("State") == "NY") &
    (F.col("Department") == "Finance") &
    (F.col("bonus") > avg_bonus_ny)
).select("employee_Name")


employees_above_avg_bonus.show()



query = """
SELECT e.employee_Name 
FROM employees e
WHERE e.State = 'NY' 
AND e.Department = 'Finance' 
AND e.bonus > (
    SELECT AVG(bonus) 
    FROM employees 
    WHERE State = 'NY'
)
"""
result = spark.sql(query)
result.show()



#9. Create DF of all those employees whose age is greater than 45 and save them in a file

from pyspark.sql import functions as F

# Assuming 'df' is your original DataFrame

# Filter the DataFrame for employees older than 45
filtered_df = df.filter(F.col("age") > 45)

# Save the filtered DataFrame as Parquet
filtered_df.write.parquet("/path/to/save/employees_over_45.parquet", mode="overwrite")

# If you prefer CSV, use the following command instead
# filtered_df.write.csv("/path/to/save/employees_over_45.csv", header=True, mode="overwrite")
