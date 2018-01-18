package cn.xiongmz.hellospark.egsql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameOperator {
	// DataFrameAPI用的不多，了解下即可。较多用的是SparkSQL
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrameOperator");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

		// 把数据框读过来完全可以理解为一张表
		Dataset<Row> df = sparkSession.read().json("src/main/resources/studentscore.json");
		
		// 打印这张表
		df.show();
		
		// 打印元数据
		df.printSchema();
		
		// 查询单列
		df.select("name").show();
		// 查询多列并计算
		df.select(df.col("name"), df.col("score").plus(1)).show();
		
		// 过滤。gt:大于。即大于80的显示
		df.filter(df.col("score").gt(80)).show();
		
		// 根据某一列分组然后count
		df.groupBy("score").count().show();
		
		sc.close();
	}
}
