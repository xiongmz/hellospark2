package cn.xiongmz.hellospark.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * 数据库连接池
 * 不完善（连接池关闭、连接池耗尽等问题），临时用
 * @author xiongmz
 *
 */
public class ConnectionPool {

	private static final String DBURL = "jdbc:mysql://10.8.40.24:3306/test_bigdata?useUnicode=true&characterEncoding=utf-8";
	private static final String DBUSER = "root";
	private static final String DBPASSWORD = "TYbd123@";

	private static LinkedList<Connection> connectionPool;
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 要加synchronized，以保障多线程安全
	 * @return
	 */
	public synchronized static Connection getConnection() {
		try {
			if (connectionPool == null) {
				connectionPool = new LinkedList<Connection>();
				for (int i = 0; i < 3; i++) {
					Connection conn = DriverManager.getConnection(DBURL, DBUSER, DBPASSWORD);
					connectionPool.push(conn);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connectionPool.poll();
	}

	public static void returnConnection(Connection conn) {
		connectionPool.push(conn);
	}
}
