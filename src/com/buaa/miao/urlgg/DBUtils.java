package com.buaa.miao.urlgg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DBUtils {

	public static void main(String[] args) {
		
		Map<String, String> map = new HashMap<>();
		DBUtils.load(map );
		System.out.println(map.size());
	}
	public static  void load(Map<String, String> map) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://192.168.25.201:3306/urlData", "root", "1234");
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select url,info from urltable");
			while(rs.next()){
				map.put(rs.getString(1), rs.getString(2));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			if(rs!=null){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stmt!=null){
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	
	}
}
