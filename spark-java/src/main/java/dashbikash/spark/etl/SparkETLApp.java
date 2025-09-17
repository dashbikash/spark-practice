package dashbikash.spark.etl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkETLApp {

	public static void main(String[] args) throws Exception {
		// Configure log4j programmatically
		
		SparkSession spark = SparkSession.builder()
				.appName("SparkETL")
				.master("local[*]")
				.getOrCreate();
		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
		Logger.getLogger("org.spark-project").setLevel(Level.ERROR);
		
		Dataset<Row> dsEmployee = spark.read()
				.option("delimiter", ",")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("escape", "\"")
				.option("quote", "\"")
				.csv("src/main/resources/employees.csv").alias("employees");
		Dataset<Row> dsDept = spark.read()
				.option("delimiter", ",")
				.option("header", "true")
				.option("inferSchema", "true")
				.option("escape", "\"")
				.option("quote", "\"")
				.csv("src/main/resources/departments.csv").alias("departments");
		dsEmployee.join(dsDept, dsEmployee.col("dept_id").equalTo(dsDept.col("id")), "left")
			.show();
	}
}