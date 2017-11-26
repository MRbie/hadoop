package com.bie.lesson03.hadoopRpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.bie.lesson03.hadoopRpc.protocol.UserLoginProtocol;

/** 
* @author  Author:别先生 
* @date Date:2017年11月26日 下午3:39:07 
*
*
*/
public class UserLoginClient {

	public static void main(String[] args) throws IOException {
		UserLoginProtocol proxy = RPC.getProxy(
				UserLoginProtocol.class, 
				11L, 
				new InetSocketAddress("localhost", 8887), 
				new Configuration());
		String login = proxy.login("张三", "123");
		System.out.println(login);
	}
}
