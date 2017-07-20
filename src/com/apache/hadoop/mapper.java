package com.apache.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	int c=0;
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
	{
		//IntWritable one = new IntWritable(1);
	    //Text word = new Text();
		System.out.println("*****************Inside mapper.java***************");
		if(c!=0)
		{
		String s="";
		String str[]=value.toString().split(",");
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		try{
		Date date = format.parse(str[0]);
		Calendar cal = new GregorianCalendar();
		cal.setTime(date);
		int m=cal.get(Calendar.MONTH)+1;
		switch(m)
	      {
	      		case 1:	s="January";		break;
	      		case 2:	s="February";		break;
	      		case 3:	s="March";			break;
	      		case 4:	s="April";			break;
	      		case 5:	s="May";			break;
	      		case 6:	s="June";			break;
	      		case 7:	s="July";			break;
	      		case 8:	s="August";			break;
	      		case 9:	s="September";		break;
	      		case 10:s="October";		break;
	      		case 11:s="November";		break;
	      		case 12:s="December";		break;
	      }
		}
		catch(Exception e)
		{
			
		}
		context.write(new Text(s), new IntWritable(1));
	}
		c++;
	}
}
