package cn.xiongmz.hellospark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * textFile只能读取UTF-8文件，如果要读取GBK文件需要另外处理
 * @author JackXiong
 *
 */
public class ReadGBKFile {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.hadoopFile("src/main/resources/gbkfile.csv", TextInputFormat.class, LongWritable.class, Text.class).map(new Function<Tuple2<LongWritable, Text>, String>() {

			private static final long serialVersionUID = 2027045734025265843L;

			@Override
			public String call(Tuple2<LongWritable, Text> p) throws Exception {
				return new String(p._2.getBytes(), 0, p._2.getLength(), "GBK");
			}
		}).foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 9071501993686240342L;

			@Override
			public void call(String arg0) throws Exception {
				System.out.println(arg0);
			}
		});
		sc.close();
	}
}
