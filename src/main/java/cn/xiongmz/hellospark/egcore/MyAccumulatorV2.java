package cn.xiongmz.hellospark.egcore;

import org.apache.spark.util.AccumulatorV2;

/**
 * 自定义累加器，是Accumulator的升级版
 * 官方实现：CollectionAccumulator, DoubleAccumulator, LegacyAccumulatorWrapper, LongAccumulator
 * 代码中可直接使用上叙官方实现或自定义实现
 * @author JackXiong
 *
 */
public class MyAccumulatorV2 extends AccumulatorV2<Integer, Integer> {

	private int num = 0;// 自定义基础变量，可以是基础类型也可以是对象类型
	/*
	 * 各重写方法必须重写，不能为默认
	 * add用于向累加器加一个值
	 * merge用于合并另一个同类型的累加器到当前累加器
	 * reset用于重置累加器
	 * value用于返回当前变量值
	 * isZero判断是否为零
	 * api文档：http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.util.AccumulatorV2
	 */

	private static final long serialVersionUID = 4051796282410546621L;

	@Override
	public void add(Integer i) {
		num = num + i + 1;
	}

	@Override
	public void reset() {
		num = 0;

	}

	@Override
	public Integer value() {
		return num;
	}

	@Override
	public AccumulatorV2<Integer, Integer> copy() {
		MyAccumulatorV2 v2 = new MyAccumulatorV2();
		v2.num = this.num;
		return v2 ;
	}

	@Override
	public boolean isZero() {

		if (num == 0) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void merge(AccumulatorV2<Integer, Integer> v2) {
		num += v2.value();
	}

}
