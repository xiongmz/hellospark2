package cn.xiongmz.hellospark.egsql;

import java.io.Serializable;

public class Student implements Serializable{
private static final long serialVersionUID = -272941271860910584L;
	
	public String id;
	public String name;
	public Integer score;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getScore() {
		return score;
	}

	public void setScore(Integer score) {
		this.score = score;
	}
}
