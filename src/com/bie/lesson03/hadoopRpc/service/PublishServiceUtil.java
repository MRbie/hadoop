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
* @author  Author:������ 
* @date Date:2017��11��26�� ����3:19:26 
*
* 1:ʹ��main������������
*/
public class PublishServiceUtil {

	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		//����һ
		
		//builder�û���������
		Builder builder = new RPC.Builder(new Configuration());
		//�󶨵ĵ�ַ
		builder.setBindAddress("localhost")
		.setPort(8888) //�˿ں�
		.setProtocol(ClientNamenodeProtocol.class)  //��ӵ�Э��
		.setInstance(new NameNodeSimulation());  //ʵ�ֵ���
		
		//�ѷ���������
		Server server = builder.build();
		server.start();
		
		//������
		//builder�û���������
		Builder builder2 = new RPC.Builder(new Configuration());
		//�󶨵ĵ�ַ
		builder2.setBindAddress("localhost")
		.setPort(8887) //�˿ں�
		.setProtocol(UserLoginProtocol.class)  //��ӵ�Э��
		.setInstance(new UserLoginService());  //ʵ�ֵ���
		
		//�ѷ���������
		Server server2 = builder2.build();
		server2.start();
	}
	
}
