package cn.xiongmz.hellospark.egsql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameCreate {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrameCreate");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		// 要求json文件每行必须是完整格式的json字符，不能内部字段嵌套，不能一行内包含2个或多个json串（如果有多个则只能识别第一个）
		Dataset<Row> ds = sparkSession.read().json("src/main/resources/studentscore.json");
		ds.show();
		sc.close();
	}
}
