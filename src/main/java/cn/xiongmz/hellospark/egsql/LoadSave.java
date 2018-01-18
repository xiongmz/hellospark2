package cn.xiongmz.hellospark.egsql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadSave {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadSave");
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		// LoadSave：统一的加载和保存
		// 只支持Parquet格式（列式数据库格式）的文件。如果需要指定其他格式的文件，需要在.read和.load中间加格式指定.format("json")
		Dataset<Row> usersDF = sparkSession.read().load("src/main/resources/users.parquet");
		usersDF.printSchema();
		usersDF.show();
		//会在工程里新建namesAndColors.parquet目录
		usersDF.select("name", "favorite_color").write().save("namesAndColors.parquet");
		
		Dataset<Row> usersDFjson = sparkSession.read().format("json").load("src/main/resources/studentscore.json");
		usersDFjson.show();
		
		sparkSession.close();
	}
}
