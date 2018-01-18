package cn.xiongmz.hellospark;

import java.util.Date;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCountByCode {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCountByCode");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Long startTime = new Date().getTime();
		// map端：行变成词
		JavaRDD<String> text = sc.textFile("E:/work/workspace/catalina.out.72W271M-1.txt", 3);// 3参数指最小的partition数量，因此读完后的partition会大于等于3
		JavaPairRDD<String, String> codes = text.mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				for(String s : line.split(",")){
					if(s.startsWith("\"code\":\"")){
						return new  Tuple2<String, String>(s.substring(8, s.length()-1), "1");
					}
				}
				return new  Tuple2<String, String>("NULL", "1");
			}
			
		});
		Map<String, Long> counts = codes.countByKey();
		Long endTime = new Date().getTime();
		for (Map.Entry<String, Long> student : counts.entrySet()) {
			if(!"NULL".equals(student.getKey())){
				System.out.println(student.getKey() + ":" + student.getValue());
			}
		}
		System.out.println("耗时："+(endTime-startTime)/1000D);
		sc.close();
	}

}
