package com.bie.lesson03.hadoopRpc.protocol;
/** 
* @author  Author:别先生 
* @date Date:2017年11月26日 下午3:14:23 
*
* 1:用户端和namenode之前通信的一种协议。
* 	这个协议里面定义了一个方法，这个方法就是用户端来调用的。
* 	用户端产生的动态代理对象也就有了这个方法，这样用户端的proxy就有了这个方法
*/
public interface ClientNamenodeProtocol {
	
	//会读取这个版本号，但是可以和客户端的不一样，没有校验
	public static final long versionID = 1L;
	public String getMetaData(String path);
}
