package com.bie.lesson03.hadoopRpc.service;

import com.bie.lesson03.hadoopRpc.protocol.ClientNamenodeProtocol;

/** 
* @author  Author:������ 
* @date Date:2017��11��26�� ����3:09:15 
*
* 1:ģ��nameode��ҵ�񷽷�֮һ
* 	��ѯԪ���ݡ�
* 2:hadoop��rpc��ܡ�
* 	RPC��Remote Procedure Call������Զ�̹��̵��ã�
* 	����һ��ͨ�������Զ�̼����������������񣬶�����Ҫ�˽�ײ����缼����Э��
*/
public class NameNodeSimulation implements ClientNamenodeProtocol{

	//ģ��nameode��ҵ�񷽷�֮һ:��ѯԪ���ݡ�
	@Override
	public String getMetaData(String path){
		//ģ��ĸ��������Ϳ�block.
		return path + ":3 - {block1,block2......}";
	}
}
