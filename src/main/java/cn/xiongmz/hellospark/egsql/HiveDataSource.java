package cn.xiongmz.hellospark.egsql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveDataSource {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("HiveDataSource");//.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// enableHiveSupport()    那么sql为hivesql
		SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		
		//由于Hive数据源需要hive-site的配置，因此必须在远端执行，****不能在本地运行****
		//远端命令：sh /opt/app/spark-2.1.0-bin-hadoop2.6/bin/spark-submit --class cn.xiongmz.hellospark.egsql.HiveDataSource --master yarn-client /opt/usrSparkApps/test.jar
		sparkSession.sql("DROP TABLE IF EXISTS student_infos");
		sparkSession.sql("CREATE TABLE IF NOT EXISTS student_infos(name STRING , age INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
		// 将student_infos.txt和student_scores.txt放在程序jar所在主机上（不是hdfs中），txt中是非json格式数据，格式为以逗号分隔，例如：zs1,19
		sparkSession.sql("LOAD DATA LOCAL INPATH '/opt/usrSparkApps/student_infos.txt' INTO TABLE student_infos");
		//hiveContext.sql("LOAD DATA INPATH 'hdfs://mycluster/usr/file/student_infos.txt' INTO TABLE student_infos");

		sparkSession.sql("DROP TABLE IF EXISTS student_scores");
		sparkSession.sql("CREATE TABLE IF NOT EXISTS student_scores(name STRING,score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
		sparkSession.sql("LOAD DATA LOCAL INPATH '/opt/usrSparkApps/student_scores.txt' INTO TABLE student_scores");
		//hiveContext.sql("LOAD DATA INPATH 'hdfs://mycluster/usr/file/student_scores.txt' INTO TABLE student_scores");

		// 将结果数据存到Hive中 todo
		Dataset<Row> rows1 = sparkSession.sql("SELECT * FROM student_infos");
		rows1.show();
		for (Row row : rows1.collectAsList()) {
			System.out.println("---1 " + row);
		}

		Dataset<Row> rows2 = sparkSession.sql("SELECT * FROM student_scores");
		rows2.show();
		for (Row row : rows2.collectAsList()) {
			System.out.println("---2 " + row);
		}
		
		//关联2张表，查询成绩大于80分的学生信息
		Dataset<Row> goodStudentsDF = sparkSession
				.sql("SELECT si.name,si.age,ss.score FROM student_infos si join student_scores ss on si.name=ss.name WHERE ss.score >= 80");
		
		List<Row> rows3 = goodStudentsDF.collectAsList();
		for (Row row : rows3) {
			System.out.println("---3 " + row);
		}

		sc.close();
		sparkSession.close();
	}
}
