package cn.xiongmz.hellospark.egoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
/**
 * partition缩减
 * @author JackXiong
 *
 */
public class CoalesceOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Coalesce算子功能是将RDD的partition缩减、减少！将一定的数据压缩到更少的partition中去
		// 使用场景：往往在filter之后紧跟Coalesce，改善数据倾斜
		List<String> staffList = Arrays.asList("xuruyun1","xuruyun2","xuruyun3","xuruyun4","xuruyun5","xuruyun6"
				,"xuruyun7","xuruyun8","xuruyun9","xuruyun10","xuruyun11","xuruyun12");
		JavaRDD<String> staffRDD = sc.parallelize(staffList, 6);
		JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = -4851783524424318263L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				List<String> list = new ArrayList<>();
				while(iterator.hasNext()){
					String staff = iterator.next();
					list.add("部门["+(index+1)+"]"+staff);
				}
				return list.iterator();
			}
		}, true);
		for(String staff : staffRDD2.collect()){
			System.out.println(staff);
		}
		
		JavaRDD<String> staffRDD3=staffRDD2.coalesce(3);// coalesce 处理。另外一个参数shuffle默认为fale，当为false时不允许进行网络传输，因此只能在节点内部进行处理
		
		JavaRDD<String> staffRDD4 = staffRDD3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = -4851783524424318263L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				List<String> list = new ArrayList<>();
				while(iterator.hasNext()){
					String staff = iterator.next();
					list.add("新部门["+(index+1)+"]"+staff);
				}
				return list.iterator();
			}
		}, true);
		for(String staff : staffRDD4.collect()){
			System.out.println(staff);
		}
		// 循环打印RDD内容也可以用foreach，区别是foreach是在work集群里面执行，collect是将数据全部加载到driver本地来执行。集群模式中worker里打印的日志在ssh和log中看不到
		// 因此collect要慎用，特别是数据量大的时候
		sc.close();
	}

}
