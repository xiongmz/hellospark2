package cn.xiongmz.hellospark.egsql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 开窗函数：即使用SparkSQL实现分组取TopN的目的
 * 此函数只对Hive有效，不能用在MySQL上
 * @author JackXiong
 *
 */
public class RowNumberWindowFunction {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RowNumberWindowFunction");
		conf.set("spark.default.parallism", "1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		//HiveSQL必须在远端执行，远端命令：sh /opt/app/spark-2.1.0-bin-hadoop2.6/bin/spark-submit --class cn.xiongmz.hellospark2.egsql.RowNumberWindowFunction --master yarn-client /opt/usrSparkApps/test.jar
		sparkSession.sql("DROP TABLE IF EXISTS sales");
		sparkSession.sql("CREATE TABLE IF NOT EXISTS sales (product STRING, category STRING, revenue BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
		sparkSession.sql("LOAD DATA LOCAL INPATH '/opt/usrSparkApps/sales.txt' INTO TABLE sales");
		// row_number()开窗函数的作用：
		// 给每个分组的数据按照排序顺序打上一个分组内的行号
		// 比如一个分组date=2017010里有三行数据，11，12，13
		// 那么对这个分组的每一行使用row_number()后会对这三行分别打上行号，行号从1开始递增，例如：11 1,12 2, 13 3
		Dataset<Row> top2SalesDF = sparkSession.sql(
				" SELECT product,category,revenue FROM "+
				" (SELECT product,category,revenue, row_number() over (PARTITION BY category ORDER BY revenue desc) rank FROM sales) tmp_sales "+
				" WHERE rank <=2 "
				);
		top2SalesDF.show();
		sc.close();
	}
}
