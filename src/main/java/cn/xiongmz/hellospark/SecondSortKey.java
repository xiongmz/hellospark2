package cn.xiongmz.hellospark;

import java.io.Serializable;

import scala.math.Ordered;
/**
 * 由于call方法在从节点执行，因此涉及到网络传输，因此必须实现序列化
 * @author JackXiong
 *
 */
public class SecondSortKey implements Serializable, Ordered<SecondSortKey> {

	private static final long serialVersionUID = 1L;

	private Integer first;

	private Integer second;

	public SecondSortKey(Integer first, Integer second) {
		super();
		this.first = first;
		this.second = second;
	}

	public Integer getFirst() {
		return first;
	}

	public void setFirst(Integer first) {
		this.first = first;
	}

	public Integer getSecond() {
		return second;
	}

	public void setSecond(Integer second) {
		this.second = second;
	}

	@Override
	public String toString() {
		return "SecondSortKey [first=" + first + ", second=" + second + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;//((first == null) ? 0 : first.hashCode());
		result = prime * result + second;//((second == null) ? 0 : second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondSortKey other = (SecondSortKey) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	/**
	 * 大于
	 */
	@Override
	public boolean $greater(SecondSortKey that) {
		if(this.first > that.getFirst()){
			return true;
		}else if(this.first == that.getFirst() && this.second > that.getSecond()){
			return true;
		}else return false;
	}
	/**
	 * 大于等于
	 */
	@Override
	public boolean $greater$eq(SecondSortKey that) {
		if(this.$greater(that)){
			return true;
		}else if(this.first == that.getFirst() && this.second == that.getSecond()){
			return true;
		}else return false;
	}
	/**
	 * 小于
	 */
	@Override
	public boolean $less(SecondSortKey that) {
		if(this.first < that.getFirst()){
			return true;
		}else if(this.first == that.getFirst() && this.second < that.getSecond()){
			return true;
		}else return false;
	
	}
	/**
	 * 小于等于
	 */
	@Override
	public boolean $less$eq(SecondSortKey that) {
		if(this.$less(that)){
			return true;
		}else if(this.first == that.getFirst() && this.second == that.getSecond()){
			return true;
		}else return false;
	}

	@Override
	public int compare(SecondSortKey that) {
		if(this.first - that.getFirst() != 0){
			return this.first - that.getFirst();
		}else{
			return this.second - that.getSecond();
		}
	}

	@Override
	public int compareTo(SecondSortKey that) {
		return compare(that);
	}
	
}
