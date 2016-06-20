package com.buaa.miao.util;

import org.apache.commons.lang.StringUtils;

public class StringTest {

	public static void main(String[] args) {
		String str = "110.178.217.18 - - [30/May/2013:17:38:21 +0800] \"GET /home.php?mod=space&uid=17496&do=profile HTTP/1.1\" 200 10159";
		String[] fields = StringUtils.split(str," ");
		
		System.out.println(fields[5]+" "+fields[8]+" "+fields[9]);
	}

}
