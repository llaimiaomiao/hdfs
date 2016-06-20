package com.buaa.hadoop.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class LoginServer {
	public static void main(String[] args) throws IOException {
		LoginServiceInterface proxy = RPC.getProxy(LoginServiceInterface.class, 1L, new InetSocketAddress("MiaoMiao20",10000), new Configuration());
		String login = proxy.login("xiaozhu", "123456");
		System.out.println(login);
	}

}
