package com.bie.lesson03.hadoopRpc.protocol;
/** 
* @author  Author:������ 
* @date Date:2017��11��26�� ����3:14:23 
*
* 1:�û��˺�namenode֮ǰͨ�ŵ�һ��Э�顣
* 	���Э�����涨����һ��������������������û��������õġ�
* 	�û��˲����Ķ�̬�������Ҳ��������������������û��˵�proxy�������������
*/
public interface ClientNamenodeProtocol {
	
	//���ȡ����汾�ţ����ǿ��ԺͿͻ��˵Ĳ�һ����û��У��
	public static final long versionID = 1L;
	public String getMetaData(String path);
}
