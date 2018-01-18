package cn.xiongmz.hellospark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TopN {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("TopN");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("src/main/resources/topN");
		//mapToPair将值变成键值对
		JavaPairRDD<Integer, String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(String v) throws Exception {
				return new Tuple2<Integer, String>(Integer.valueOf(v), v);
			}
		});
		//sortByKey参数false 倒序从大到小，不写默认为true 正序从小到大
		//map将键值对变成值
		//take取前N个元素
		List<String> result = pairs.sortByKey(false).map(new Function<Tuple2<Integer, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, String> tuple) throws Exception {
				return tuple._2;
			}

		}).take(3);// 取3个
		for(String s : result){
			System.out.println(s);
		}
		sc.close();
	}
}
