package cn.xiongmz.hellospark.egcore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量/共享变量
 * @author JackXiong
 *
 */
public class BroadCastValue {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BroadCastValue");
		JavaSparkContext sc = new JavaSparkContext(conf);

		final int f = 3;
		final Broadcast<Integer> broadcastFactor = sc.broadcast(f);
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		JavaRDD<Integer> resultRDD = listRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;
			//broadcastFactor是只读的
			// 如果不做共享变量，那么f变量将会分发到各节点中的各task里面去。如果做了共享变量，那么每个节点只有1个副本，各task共享这个副本
			// 共享变量不影响结果，只提高效率
			@Override
			public Integer call(Integer num) throws Exception {
				// 各值乘以3
				return num * broadcastFactor.value();
			}

		});
		resultRDD.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);

			}
		});
		/**
		 * output:
		    3
			6
			9
			12
			15
		 */
		sc.close();
	}
}
