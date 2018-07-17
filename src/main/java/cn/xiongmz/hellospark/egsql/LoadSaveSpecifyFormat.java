package cn.xiongmz.hellospark.egsql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class LoadSaveSpecifyFormat {
	
	public static void main(String[] args) {
		SparkSession sparkSession = null;
		try {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadSaveSpecifyFormat");
		sparkSession = SparkSession.builder().config(conf).getOrCreate();
		// LoadSave：统一的加载和保存
		// 默认不加.format("xxx")只支持列式数据库，如果要其他类型的数据需要在.read和.load中间指定格式
		// 其他格式：json, parquet, jdbc, orc, libsvm, csv, text
		Dataset<Row> usersDF = sparkSession.read().format("json").load("src/main/resources/studentscore.json");
		usersDF.printSchema();
		usersDF.show();
		
		//write时指定格式：.format
		usersDF.select("name").write().format("json").save("studentscore.json");// 将name列按json格式单独保存
		usersDF.select("name").write().format("parquet").save("studentscore.parquet");// 将name列按parquet格式单独保存
		// 可指定保存模式
		// SaveMode.ErrorIfExists(default) 如果数据已经存在,则会抛出异常
		// SaveMode.Append  如果数据已经存在,则DataFrame的内容将被append（附加）到现有数据中.
		// SaveMode.Overwrite 如果数据已经存在，预期DataFrame的内容将overwritten（覆盖）现有数据.包括表格式。因此此模式下不需要预先创建表
		// SaveMode.Ignore	如果数据已经存在,则不会保存DataFrame的内容,并且不更改现有数据.这与SQL中的CREATE TABLE IF NOT EXISTS类似.
		usersDF.select("name").write().mode(SaveMode.Append).format("json").save("studentscore2.json");// 将name列单独保存

		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(sparkSession!=null) {
				sparkSession.stop();
			}
		}
	}
}
