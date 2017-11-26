package com.bie.lesson03.hadoopRpc.service;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import com.bie.lesson03.hadoopRpc.protocol.ClientNamenodeProtocol;
import com.bie.lesson03.hadoopRpc.protocol.UserLoginProtocol;

/** 
* @author  Author:别先生 
* @date Date:2017年11月26日 下午3:19:26 
*
* 1:使用main方法发布服务
*/
public class PublishServiceUtil {

	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		//案例一
		
		//builder用户构建服务
		Builder builder = new RPC.Builder(new Configuration());
		//绑定的地址
		builder.setBindAddress("localhost")
		.setPort(8888) //端口号
		.setProtocol(ClientNamenodeProtocol.class)  //遵从的协议
		.setInstance(new NameNodeSimulation());  //实现的类
		
		//把服务跑起来
		Server server = builder.build();
		server.start();
		
		//案例二
		//builder用户构建服务
		Builder builder2 = new RPC.Builder(new Configuration());
		//绑定的地址
		builder2.setBindAddress("localhost")
		.setPort(8887) //端口号
		.setProtocol(UserLoginProtocol.class)  //遵从的协议
		.setInstance(new UserLoginService());  //实现的类
		
		//把服务跑起来
		Server server2 = builder2.build();
		server2.start();
	}
	
}
