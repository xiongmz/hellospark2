package cn.xiongmz.hellospark.egsql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * 将源数据保存到HIVE中
 * 推荐：insertInto方式
 * @author xiongmz
 *
 */
public class LoadSave2Hive {

	public static void main(String[] args) {
		SparkSession sparkSession = null;
		try {
			SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadSave2Hive");
			// 保存到HIVE中必须使用.enableHiveSupport()。要定义DF Schema（Encoder）
			sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
			Encoder<StudentScore> studentScoreEncoder = Encoders.bean(StudentScore.class);
//			String filePath = "src/main/resources/studentscore.json";// win local
			// linux remote
			String filePath = "file:///opt/xmz/studentscore.json";
			Dataset<StudentScore> usersDF = sparkSession.read().json(filePath).as(studentScoreEncoder);
			
			////////////////////////    saveAsTable
			// 如果不指定format，那么存储的是Parquet格式文件，可以正常sparksql和hive命令查询
			// 如果指定format为Text，那么存储的是txt格式文件，要注意：（Text data source supports only a single column）。因此需要.select("colname")
			//		text表保存的表结构：col                 	array<string>       	from deserializer
			//		txt格式在sparkSession可以用sql语句查询，但是在hive命令行中不能查询，会报错.txt not a SequenceFile
			// 如果指定format为json，那么存储的是json格式文件，
			//   	json表保存的表结构：col                 	array<string>       	from deserializer
			//		json格式在sparkSession可以用sql语句查询，但是在hive命令行中不能查询，会报错.json not a SequenceFile
			usersDF.write().format("json").mode(SaveMode.Overwrite).saveAsTable("TEST_SAVEDFTOHIVE");
			sparkSession.sql("select * from TEST_SAVEDFTOHIVE").show();
			
			/////////////////////////////    insetinto 
			/*
			create table TEST_SAVEDFTOHIVE2(
				name STRING,
				score STRING
			)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
			 */
			//要显示指定数据库
			// hive-hdfs上保存的是正常的文本文件part-00000，可以UE打开。可以hive命令行查询
			sparkSession.sql("TRUNCATE TABLE default.TEST_SAVEDFTOHIVE2");
			usersDF.write().insertInto("default.TEST_SAVEDFTOHIVE2");
			
			/////////////////////////////    insetinto2
			// 还可以df创建临时表之后，insert into table select * from 临时表 
			/*
			 * create table TEST_SAVEDFTOHIVE3(
				name STRING,
				score STRING
			)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
			 */
			usersDF.createOrReplaceTempView("TEST_SAVEDFTOHIVETEMP");
			sparkSession.sql("TRUNCATE TABLE default.TEST_SAVEDFTOHIVE3");
			sparkSession.sql("INSERT INTO default.TEST_SAVEDFTOHIVE3 SELECT * FROM TEST_SAVEDFTOHIVETEMP");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sparkSession != null) {
				sparkSession.stop();
			}
		}
	}
}
