package cn.xiongmz.hellospark.egsql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RDD2DataFrameDynamic {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameDynamic");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		// Dynamic动态，主要体现在schema（bean字段）不是写死的
		JavaRDD<String> lines = sc.textFile("src/main/resources/student.txt");
		JavaRDD<Row> rows = lines.map(new Function<String, Row>() {

			private static final long serialVersionUID = -9208114663905894223L;

			@Override
			public Row call(String line) throws Exception {
				String[] array = line.split(",");
				return RowFactory.create(array[0], array[1], Integer.valueOf(array[2]));
			}
		});
		// 动态构造元数据，还有一种方式是通过反射的方式来构建出Dataframe，这里用的是动态创建元数据
		// 场景：有些时候我们一开始不确定有哪些列，而这些列是需要从数据库比如mysql或者配置文件加载出来
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> scoreDF = sparkSession.createDataFrame(rows, schema);
		scoreDF.createOrReplaceTempView("student");
		Dataset<Row> scoreDF2 = sparkSession.sql("select * from student where score <93");

		List<Row> list = scoreDF2.javaRDD().collect();
		for (Row stu : list) {
			System.out.println(stu);
		}
		sc.close();
	}
}
