package com.buaa.miao.urldo;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopoUrlMap extends Mapper<LongWritable, Text, Text, FlowBean>{
		
		private long id =0;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				String line = value.toString();
				String[] fields = StringUtils.split(line," ");
				try{
				if(fields.length>9 &&StringUtils.isNotEmpty(fields[5])&&fields[5].equals("\"GET")){
				id++;
				String url = fields[5]+id;
				long upFlow = Long.parseLong(fields[8]);
				long dFlow = Long.parseLong(fields[9]);
				context.write(new Text(url), new FlowBean(upFlow, dFlow));
				}
				}catch(Exception e){
					System.out.println("Exception is ignored");
				}
		
		}

}
