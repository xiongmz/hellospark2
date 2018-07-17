package cn.xiongmz.hellospark.egsql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * RDD --> Dataset
 * 使用反射。RDD<T>和T对应的JavaBean进行映射，以转换成Dataset
 * 不支持含有Map的JavaBeans. 但是支持嵌套List或者 ArrayJavaBeans.
 * @author xiongmz
 *
 */
public class RDD2DataFrameReflection {
	public static void main(String[] args) {
		JavaSparkContext sc = null;
		SparkSession sparkSession = null;
		try {
			SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection");
			sc = new JavaSparkContext(conf);
			sparkSession = SparkSession.builder().config(conf).getOrCreate();
			// 将RDD转成DataFrame
			// Student不能是内部类或者无修饰符的类，必须是独立的常规的public class文件
			JavaRDD<String> linesRDD = sc.textFile("src/main/resources/student.txt");
			JavaRDD<Student> studentRDD = linesRDD.map(new Function<String, Student>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Student call(String line) throws Exception {
					String[] lineSplit = line.split(",");
					Student stu = new Student();
					stu.setId(lineSplit[0]);
					stu.setName(lineSplit[1]);
					stu.setScore(Integer.parseInt(lineSplit[2]));
					return stu;
				}
			});
			// 使用反射的方式将RDD转化成DataFrame
			Dataset<Row> studentDF = sparkSession.createDataFrame(studentRDD, Student.class);
			// 使用反射的方式将RDD转化成DataFrame			
			
			studentDF.printSchema();
			// 有了DF就可以注册一个临时表，用sql查询成绩小于93的人
			studentDF.createOrReplaceTempView("student");
			Dataset<Row> scoreDF = sparkSession.sql("select * from student where score < 93");

			JavaRDD<Row> scoreRDD = scoreDF.toJavaRDD();
			JavaRDD<Student> stuRDD = scoreRDD.map(new Function<Row, Student>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Student call(Row row) throws Exception {
					// 通过反射来生成这个DataFrame的方式如果用get（index），大家要注意这个列的顺序是字典顺序
					// String id = row.getInt(1);
					// String name = row.getString(2);
					// int score = row.getInt(0));

					// 通过列名来从row里取数据，这样比较精准，不会有因为顺序而去错的情况
					String id = row.getAs("id");
					String name = row.getAs("name");
					int score = row.getAs("score");
					
					Student stu = new Student();
					stu.setId(id);
					stu.setName(name);
					stu.setScore(score);
					return stu;
				}
			});
			stuRDD.foreach(new VoidFunction<Student>() {

				private static final long serialVersionUID = -3580591107424710508L;

				@Override
				public void call(Student t) throws Exception {
					System.out.println(t.getId() + " " + t.getName() + " " + t.getScore());
				}
			});
			List<Student> list = stuRDD.collect();
			for(Student t : list){
				System.out.println(t.getId()+" "+t.getName()+" "+t.getScore());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sc != null) {
				sc.stop();
			}
			if (sparkSession != null) {
				sparkSession.stop();
			}
		}

	}
}
