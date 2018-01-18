package cn.xiongmz.hellospark.egoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 基于MapPartitions，只是多提供了api可以知道每个分区的下标
 * @author JackXiong
 *
 */
public class MapPartitionsWithIndexOperator {
	// 要理解final使用的原因（算子外面的变量需要定义成final，否则在算子内部不能引用。涉及网络传输，因此不希望算子引用时变量被改动）
	public static void main(String[] args) {
		// 以下代码在driver端执行
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> names = Arrays.asList("xurunyuan", "liangyongqi", "wangfei");// 数据来源为数组

		JavaRDD<String> nameRDD = sc.parallelize(names, 2);
		// parallelize中多了一个参数2代表并行度为2（默认是取local后接的参数 local[2]），即numPartitions是2。local后参数默认为1：local[1]
		// 至于数组中的内容怎么分到partition中去由spark决定
		// 如果想知道谁和谁分到一组去了，可以通过MapPartitionsWithIndex算子得到partition的下标
		// 自定义partition属于优化的内容，可以根据实际主机配置及数据量来选择合适的值

		JavaRDD<String> nameWithPartitionIndex = nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = -206293690553627706L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				List<String> list = new ArrayList<>();
				while (iterator.hasNext()) {
					String name = iterator.next();
					String result = index + ":" + name;
					list.add(result);
				}
				return list.iterator();
			}
		}, true);
		nameWithPartitionIndex.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 6705927365493327232L;

			@Override
			public void call(String result) throws Exception {
				System.out.println(result);
			}
		});
		sc.close();
	}

}
