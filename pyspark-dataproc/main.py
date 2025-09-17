from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("EmployeeDepartmentJoin").config("master", "local[*]").getOrCreate()
    df_employee = spark.read.csv("data/employee.csv", header=True, inferSchema=True).alias("emp")
    df_department = spark.read.csv("data/dept.csv", header=True, inferSchema=True).alias("dept")
    df_full = df_employee.join(df_department, df_employee.dept_id == df_department.id, "inner")
    df_full.select(count("emp.id").alias("total_records")).show()

if __name__ == "__main__":
    main()