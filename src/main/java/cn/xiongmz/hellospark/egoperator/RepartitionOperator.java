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
 * 增加或减少分区数
 * @author JackXiong
 *
 */
public class RepartitionOperator {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		/*
		 * Repartition算子，用于任意将RDD中的partition增加或减少
		 * Coalesce算子一般来说只能将RDD中的partition减少
		 * 从源码来看，Repartition直接调Coalesce算子，只是Coalesce的shuffle参数为true
		 * 源码注释中已说明：减少时用Coalesce，增多时用Repartition。因为Repartition有shuffle动作因此减少的时候用Coalesce效率最高
		 * 建议使用场景：
		 * SparkSQL查询HIVE时，SparkSQL会根据对应的HDFS文件的block数量来决定RDD中partition数量
		 * 这里默认的partition数量是无法更改的，但是有时这个默认值太少，为了优化，可以进行调整以提高并行度
		 */
		// 公司要增加部门
		List<String> staffList = Arrays.asList(
				"xuruyun1","xuruyun2","xuruyun3","xuruyun4","xuruyun5",
				"xuruyun6","xuruyun7","xuruyun8","xuruyun9","xuruyun10","xuruyun11","xuruyun12");
		JavaRDD<String> staffRDD = sc.parallelize(staffList, 3 );//3个partition
		JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(
				new Function2<Integer, Iterator<String>, Iterator<String>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
						List<String> list = new ArrayList<String>();
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
		
		JavaRDD<String> staffRDD3=staffRDD2.repartition(6);// 增加为6个partition
		
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
		// 循环打印RDD内容也可以用foreach，区别是foreach是在work集群里面执行，collect是将数据全部加载到driver本地来执行
		// 因此collect要慎用，特别是数据量大的时候
		sc.close();
	}

}
