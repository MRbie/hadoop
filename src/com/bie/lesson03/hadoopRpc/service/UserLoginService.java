package com.bie.lesson03.hadoopRpc.service;

import com.bie.lesson03.hadoopRpc.protocol.UserLoginProtocol;

/** 
* @author  Author:别先生 
* @date Date:2017年11月26日 下午3:37:22 
*
*
*/
public class UserLoginService implements UserLoginProtocol{

	@Override
	public String login(String account, String password) {
		
		return "账号:" + account + "登陆成功..... ,密码" + password;
	}

	
}
