package cn.xiongmz.hellospark.egsql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadSaveSpecifyFormat {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadSaveSpecifyFormat");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		// LoadSave：统一的加载和保存
		// 默认不加.format("xxx")只支持列式数据库，如果要其他类型的数据需要指定格式
		Dataset<Row> usersDF = sparkSession.read().format("json").load("src/main/resources/studentscore.json");
		usersDF.printSchema();
		usersDF.show();
		//write时指定格式：.format
		usersDF.select("name").write().format("json").save("studentscore.json");// 将name列单独保存
		usersDF.select("name").write().format("parquet").save("studentscore.parquet");// 将name列单独保存
		sc.close();
	}
}
