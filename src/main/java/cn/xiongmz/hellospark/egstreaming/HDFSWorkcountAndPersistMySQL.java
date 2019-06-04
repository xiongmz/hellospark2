package cn.xiongmz.hellospark.egstreaming;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import cn.xiongmz.hellospark.common.ConnectionPool;
import scala.Tuple2;

/**
 * 以HDFS作为数据源，并奖结果持久化到MySQL中
 * 隔一段时间从HDFS上读取新文件来计算
 * @author xiongmz
 *
 */
public class HDFSWorkcountAndPersistMySQL {
	
	// 在远端运行。在本机运行的话需要将hdfs路径指定为单点，例如：hdfs://nd1:8020/xiongmz
	// 程序启动后不会先把HDFS文件夹下已有历史数据进行第一次处理，然后监听新增数据
	// 只要文件日期是新的，就会触发执行。即使文件内容不变。
	// 同一时间间隔内，对一个文件先删除，再同名上传（即使修改文件内容）。不会触发执行
	/*
	 * 运行演示说明：
	 * 远端运行，然后hdfs上传文件hdfs dfs -put /opt/xmz/wc.txt /xiongmz
	 */
	public static void main(String[] args) {
		JavaStreamingContext jssc = null;
		try {
			// 文件流不需要运行一个接收器（receiver），因此，不需要额外分配内核
			// 由于不像Socket一样需要ReceiverInputDStream启用线程来监听数据源，因此不强制要求线程数[2]大于等于2，设置为1就可以运行。
			SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWorkcount");
			jssc = new JavaStreamingContext(conf, Durations.seconds(5));
			/**
			 * textFileStream(dataDirectory)
			 * Spark Streaming 将监控dataDirectory 目录并且该目录中任何新建的文件 (写在嵌套目录中的文件是不支持的)
			 * 文件必须具有相同的数据格式
			 * 文件必须被创建在 dataDirectory 目录中，
			 * 文件必须不能再更改，因此如果文件被连续地追加，新的数据将不会被读取。
			 */
			// from hdfs
			JavaDStream<String> lines = jssc.textFileStream("hdfs://mycluster/xiongmz");
			
			JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
				private static final long serialVersionUID = 5986920588831791764L;
				@Override
				public Iterator<String> call(String line) throws Exception {
					return Arrays.asList(line.split(" ")).iterator();
				}
			});
			JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
				private static final long serialVersionUID = 4819478427121057001L;
				@Override
				public Tuple2<String, Integer> call(String word) throws Exception {
					return new Tuple2<String, Integer>(word, 1);
				}
			});
			JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = -903037400730733949L;
				@Override
				public Integer call(Integer n1, Integer n2) throws Exception {
					return n1+n2;
				}
			});
			wordcounts.print();//action操作。打印前10行
			
			//保存到MySQL中
			wordcounts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
				private static final long serialVersionUID = -5870236878087187210L;
				@Override
				public void call(JavaPairRDD<String, Integer> wordcountsRDD) throws Exception {
					// 优化策略：每个Partition执行一次，而不是每个元素一次
					wordcountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {
						private static final long serialVersionUID = -8938099909395643398L;
						@Override
						public void call(Iterator<Tuple2<String, Integer>> wordcounts) throws Exception {
							Connection conn = ConnectionPool.getConnection();
							Tuple2<String, Integer> wordcount = null;
							while(wordcounts.hasNext()) {
								wordcount = wordcounts.next();
								String sql = "INSERT INTO WORDCOUNT(WORD,COUNT)VALUES(?,?)";
								PreparedStatement pstmt = conn.prepareStatement(sql);
								pstmt.setString(1, wordcount._1);
								pstmt.setInt(2, wordcount._2);
								pstmt.executeUpdate();
							}
							ConnectionPool.returnConnection(conn);
						}
					});
				}
			});
			// 固定行
			jssc.start();
			jssc.awaitTermination();
			// 固定行
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (jssc != null) {
				jssc.stop();
			}
		}
	}
}
