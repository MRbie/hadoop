package com.bie.lesson03.hadoopRpc.service;

import com.bie.lesson03.hadoopRpc.protocol.UserLoginProtocol;

/** 
* @author  Author:������ 
* @date Date:2017��11��26�� ����3:37:22 
*
*
*/
public class UserLoginService implements UserLoginProtocol{

	@Override
	public String login(String account, String password) {
		
		return "�˺�:" + account + "��½�ɹ�..... ,����" + password;
	}

	
}
