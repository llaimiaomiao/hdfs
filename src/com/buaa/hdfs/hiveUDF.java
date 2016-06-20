package com.buaa.hdfs;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

public class hiveUDF extends UDF{
	private static Map<String,Object> map = new HashMap<>();
	static{
		map.put("1366", "beijing");
		map.put("1377", "shanghai");
		map.put("1388", "tianjin");
		map.put("1399", "najing");
	}
	public String evaluate(String phoneNum) {
		
		return (String) map.get(phoneNum.substring(4))==null? phoneNum+" huoxing" :phoneNum+" "+(String) map.get(phoneNum.substring(4));
	}
}
