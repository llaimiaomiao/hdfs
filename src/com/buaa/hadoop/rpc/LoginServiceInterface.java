package com.buaa.hadoop.rpc;

public interface LoginServiceInterface {
	public static final long versionID = 1L;
	public String login(String username,String password);

}
