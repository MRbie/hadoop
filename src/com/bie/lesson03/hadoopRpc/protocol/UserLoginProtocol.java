package com.bie.lesson03.hadoopRpc.protocol;
/** 
* @author  Author:������ 
* @date Date:2017��11��26�� ����3:35:47 
*
* 1:��½Э��
*/
public interface UserLoginProtocol {

	public static final long versionID = 11L;
	public String login(String account,String password);
}
