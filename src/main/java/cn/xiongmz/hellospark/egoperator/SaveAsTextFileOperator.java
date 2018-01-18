package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * 将RDD保存为本地文件
 * @author JackXiong
 *
 */
public class SaveAsTextFileOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);
		JavaRDD<Integer> doubleNumbersRDD = numbersRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				return v * 2;
			}
			
		});
		// 存储路径说明：
		// 路径必须不存在，否则再次存入时会报错
		// 不建议使用.coalesce(1)之后在save（以保障只存储一个part-文件），对于小数据量还好，大数量的话涉及很多的IO来进行分区合并甚至造成driver端内存溢出。建议对多个part结果文件进行hdfs getmerge合并来实现
		// win中local模式下， saveAsTextFile("E:/123")，E盘123文件夹下会有part-0000文件
		// linux中yarn-client模式下，saveAsTextFile("/opt/xmz/123")，那么在hdfs下（不是linux本地）会自动递归建立/opt/xmz/123/文件夹，文件夹下有part-0000文件。等价于hdfs://mycluster/opt/xmz/123
		// linux中yarn-client模式下，saveAsTextFile("file:/opt/xmz/123")，那么在linuxDriver本地会建立/opt/xmz/123/文件夹，里面有SUCCESS状态文件，真正的结果文件part-0000x在executor里面task文件夹下（/opt/xmz/123/_temporary/0/task_20180109091525_0000_m_000000part-00000）
		doubleNumbersRDD.saveAsTextFile("E:/123");
		sc.close();
	}
}
