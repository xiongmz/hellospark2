package cn.xiongmz.hellospark.egsql;

import java.io.Serializable;

public class StudentScore implements Serializable{
	
	private static final long serialVersionUID = -7101581079256859938L;
	public String name;
	public Integer score;

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
