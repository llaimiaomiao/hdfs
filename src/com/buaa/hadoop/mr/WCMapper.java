package com.buaa.hadoop.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
			String str = value.toString();
			String[] words = StringUtils.split(str," ");
			for(String word:words){
				
				context.write(new Text(word), new LongWritable(1));
			}
	}

}
