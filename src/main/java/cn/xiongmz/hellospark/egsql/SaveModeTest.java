package cn.xiongmz.hellospark.egsql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SaveModeTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SaveModeTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> usersDF = sparkSession.read().format("json").load("src/main/resources/studentscore.json");

		usersDF.write().format("json").mode(SaveMode.ErrorIfExists).save("studentscore.json");// 如果文件夹studentscore.json存在则报错
		usersDF.write().format("json").mode(SaveMode.Append).save("studentscore.json");// 如果文件夹studentscore.json存在则在文件夹在再新建一个文件来存
		usersDF.write().format("json").mode(SaveMode.Ignore).save("studentscore.json");// 如果文件夹studentscore.json存在则不存，也不报错
		usersDF.write().format("json").mode(SaveMode.Overwrite).save("studentscore.json");// 如果文件夹studentscore.json存在则覆盖

		sc.close();
		sparkSession.close();
	}
}
