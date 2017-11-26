package com.bie.lesson03.hadoopRpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.bie.lesson03.hadoopRpc.protocol.ClientNamenodeProtocol;

/** 
* @author  Author:������ 
* @date Date:2017��11��26�� ����3:27:00 
*
* 1:hdfs�Ŀͻ���
* 2:hadoop��rpc������ط���һ������Զ�̵ġ�
*/
public class HdfsClient {
 
	public static void main(String[] args) throws IOException {
		//rpc�õ���̬�������,Ȼ��ͨ����������
		ClientNamenodeProtocol proxy = RPC.getProxy(
				ClientNamenodeProtocol.class, 
				1L, 
				new InetSocketAddress("localhost",8888), 
				new Configuration());
		//ģ���ȡ�����������Ϳ�block
		//�����������е�����
		String metaData = proxy.getMetaData("/ģ��.txt");
		System.out.println(metaData);
	}
}
