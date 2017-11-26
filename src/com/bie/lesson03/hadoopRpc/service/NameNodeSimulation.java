package com.bie.lesson03.hadoopRpc.service;

import com.bie.lesson03.hadoopRpc.protocol.ClientNamenodeProtocol;

/** 
* @author  Author:别先生 
* @date Date:2017年11月26日 下午3:09:15 
*
* 1:模拟nameode的业务方法之一
* 	查询元数据。
* 2:hadoop的rpc框架。
* 	RPC（Remote Procedure Call）——远程过程调用，
* 	它是一种通过网络从远程计算机程序上请求服务，而不需要了解底层网络技术的协议
*/
public class NameNodeSimulation implements ClientNamenodeProtocol{

	//模拟nameode的业务方法之一:查询元数据。
	@Override
	public String getMetaData(String path){
		//模拟的副本数量和块block.
		return path + ":3 - {block1,block2......}";
	}
}
