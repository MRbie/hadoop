package com.bie.lesson03.hadoopRpc.protocol;
/** 
* @author  Author:别先生 
* @date Date:2017年11月26日 下午3:35:47 
*
* 1:登陆协议
*/
public interface UserLoginProtocol {

	public static final long versionID = 11L;
	public String login(String account,String password);
}
