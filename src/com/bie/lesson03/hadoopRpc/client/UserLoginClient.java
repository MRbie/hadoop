package com.bie.lesson03.hadoopRpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.bie.lesson03.hadoopRpc.protocol.UserLoginProtocol;

/** 
* @author  Author:������ 
* @date Date:2017��11��26�� ����3:39:07 
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
		String login = proxy.login("����", "123");
		System.out.println(login);
	}
}
