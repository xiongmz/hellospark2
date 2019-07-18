package cn.xiongmz.hellospark.egsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * JDBC读取和保存
 * 参考：http://spark.apache.org/docs/2.4.0/sql-data-sources-jdbc.html
 * examples/src/main/java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java
 * @author xiongmz
 *
 */
public class JDBCDataSource {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
		SparkSession ss = SparkSession.builder().config(conf).getOrCreate();
		// 读取MySQL数据
		Map<String, String> options = new HashMap<>();
		options.put("url", "jdbc:mysql://10.8.15.14:3306/xmz_");
		options.put("user", "root");// 注意，不是username
		options.put("password", "123456");
		options.put("dbtable", "sys_user");
		Dataset<Row> sysUserDF = ss.read().format("jdbc").options(options).load();
		System.out.println("-- sys_user全表查询");
		sysUserDF.show();
		
		// 读取MySQL数据，使用Properties来保存用户密码
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123456");
		//predicates可指定where条件
		String[] predicates = new String[]{"company_id='1'"};
		Dataset<Row> sysUserDF2 = ss.read()
				  .jdbc("jdbc:mysql://10.8.15.14:3306/xmz_", "sys_user", predicates, connectionProperties);
		System.out.println("-- 按where条件查询");
		sysUserDF2.show();
		
		// 将DataFrame数据存储到MySQL表中，这在公司很常见，要掌握
		// 要引入mysql-connector-java-5.1.30.jar
		// 集群中在spark-defaults.conf中配置以下两行
		// spark.driver.extraClassPath=/path/xxx/mysql-connector-java-5.1.30.jar
		// spark.executor.extraClassPath=/path/xxx/mysql-connector-java-5.1.30.jar
		
		sysUserDF.write().mode(SaveMode.Append).format("jdbc")
			.option("url", "jdbc:mysql://10.8.15.14:3306/xmz_")
			.option("dbtable", "sys_user_copy")
			.option("user", "root")
			.option("password", "123456")
			.save();
		//也可以
		//sysUserDF.write().mode(SaveMode.Append).jdbc("jdbc:mysql://10.8.15.14:3306/xmz_", "sys_user_copy", connectionProperties);
		
		
		// 在call里面创建数据库连接将非常耗数据库资源，必须优化。见代码（从方式不推荐）：
//		sysUserDF.javaRDD().foreach(new VoidFunction<Row>() {
//			private static final long serialVersionUID = -5295500782973246042L;
//			@Override
//			public void call(Row row) throws Exception {
//				String sql = "insert into sys_user_temp values('" + row.getString(0) + "')";
//				Class.forName("com.mysql.jdbc.Driver");
//				Connection conn = null;
//				Statement stat = null;
//				try {
//					conn = DriverManager.getConnection("jdbc:mysql://10.8.15.14:3306/xmz_", "root", "123456");
//					stat = conn.createStatement();
//					stat.executeUpdate(sql);
//				} catch (Exception e) {
//					e.printStackTrace();
//				} finally {
//					if (stat != null) {
//						stat.close();
//					}
//					if (conn != null) {
//						conn.close();
//					}
//				}
//			}
//		});
	}

}
