package com.buaa.miao.urlgg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UrlGGMap extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Map<String, String> map = new HashMap<>();
		private long id =0;
		@Override
		protected void setup(Context context)
			throws IOException, InterruptedException {
			DBUtils.load(map);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line, " ");
			id++;
			String url = fields[5]+id;
			String info = map.get(url);
			if(info!=null){
				context.write(new Text(url+"--"+map.get(url)+"\n"), NullWritable.get());
			}
			else{
				context.write(new Text(url+"--"+"ISNOTFOUND!!!"+"\n"), NullWritable.get());
			}
		}

}
