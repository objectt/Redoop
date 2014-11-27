package com.boxple.redoop;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableUtils;

public class DateKey implements WritableComparable<DateKey>{
	private Integer year;
	private Integer month;
	private Integer day;

	public DateKey(){}

	public DateKey(Integer year, Integer month, Integer day){
		this.year = year;
		this.month = month + 1;
		this.day = day;
	}

	public void set(DateKey datekey){
		this.year = datekey.getYear();
		this.month = datekey.getMonth();
		this.day = datekey.getDay();
	}

	public void setYear(Integer year){
		this.year = year;
	}

	public Integer getYear(){
		return year;
	}
	
	public Integer getMonth(){
		return month;
	}

	public void setMonth(Integer month){
		this.month = month;
	}

	public Integer getDay(){
		return day;
	}

	public void setDay(Integer day){
		this.day = day;
	}

	@Override
	public String toString(){
		String monthStr = String.valueOf(month);
		String dayStr = String.valueOf(day);

		if(month < 10){
			monthStr = new StringBuilder().append(0).append(monthStr).toString();
		} if(day < 10) {
			dayStr = new StringBuilder().append(0).append(dayStr).toString();
		}

		return (new StringBuilder()).append(year).append("").append(monthStr).append("").append(dayStr).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		//year = WritableUtils.readString(in);
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
	}

	@Override
	public int compareTo(DateKey key){
		int result = year.compareTo(key.year);
		if(0 == result){
			result = month.compareTo(key.month);
			if(0 == result)
				result = day.compareTo(key.day);
		}

		return result;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj == null)
			return false;
        if (getClass() != obj.getClass())
            return false;
        
		final DateKey other = (DateKey) obj;
		if(other.compareTo(this) != 0)
			return false;
		
		return true;
	}
	
    @Override
    public int hashCode() {
    	return Integer.parseInt(toString());
    }
}