package com.bie.dataStream.hive.bean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

/** 
* @author  Author:������ 
* @date Date:2017��12��16�� ����3:48:28 
*
* 1:������־��
* 	����js/ͼƬ/css�Ⱦ�̬��Դ��
* 
*/
public class WebLogParser {
 
	//ʱ��ĸ�ʽ1�͸�ʽ2,df1Դ���ݸ�ʽ��dfsĿ���ʽ��
	public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

	/**
	 * ����ÿһ������
	 * @param line
	 * @return ����WebLogBean
	 */
	public static WebLogBean parser(String line) {
		//����һ����־ʵ����
		WebLogBean webLogBean = new WebLogBean();
		//�Կո���н�ȡ
		String[] arr = line.split(" ");
		//�жϣ�������ȴ���11�Ľ��н�ȡ,���򷵻�
		if (arr.length > 11) {
			// ��¼�ͻ��˵�ip��ַ
			webLogBean.setRemote_addr(arr[0]);
			// ��¼�ͻ����û�����,��������"-"
			webLogBean.setRemote_user(arr[1]);
			//��ȡ�������ֶε����ڣ�ȥ����������ݣ���[19/Sep/2013:00:31:53 +0000]
			String time_local = formatDate(arr[3].substring(1));
			//�жϣ����time_localΪnull,��ô����һ��Ĭ��ֵ
			if(null==time_local){
				time_local="-invalid_time-";
			}
			// ��¼����ʱ����ʱ��
			webLogBean.setTime_local(time_local);
			// ��¼�����url��httpЭ��
			webLogBean.setRequest(arr[6]);
			// ��¼����״̬���ɹ���200
			webLogBean.setStatus(arr[8]);
			// ��¼���͸��ͻ����ļ��������ݴ�С
			webLogBean.setBody_bytes_sent(arr[9]);
			// ������¼���Ǹ�ҳ�����ӷ��ʹ�����
			webLogBean.setHttp_referer(arr[10]);

			//���useragentԪ�ؽ϶࣬ƴ��useragent
			//������һ���ֶΣ����̲�һ������ƴ��һ��
			if (arr.length > 12) {
				//��������ֶν���ƴ��
				StringBuilder sb = new StringBuilder();
				for(int i=11;i<arr.length;i++){
					sb.append(arr[i]);
				}
				// ��¼�ͻ�������������Ϣ
				webLogBean.setHttp_user_agent(sb.toString());
			} else {
				// ��¼�ͻ�������������Ϣ
				webLogBean.setHttp_user_agent(arr[11]);
			}
			// ����400��HTTP����
			if (Integer.parseInt(webLogBean.getStatus()) >= 400) {
				webLogBean.setValid(false);
			}
			// �жϣ����time_localΪnull,�ж����ݲ��Ϸ�
			if("-invalid_time-".equals(webLogBean.getTime_local())){
				webLogBean.setValid(false);
			}
		} else {
			//false,�ж����ݲ��Ϸ�,������Ϸ�ֱ�ӷ���
			webLogBean.setValid(false);
		}
		//���Ϸ����߲��Ϸ������ݷ��ػ�ȥ��
		return webLogBean;
	}

	/***
	 * 
	 * @param bean
	 * @param pages
	 */
	public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
		//����js/ͼƬ/css�Ⱦ�̬��Դ
		//���ҳ�治������Щ��������Ϊ���Ϸ�
		if (!pages.contains(bean.getRequest())) {
			bean.setValid(false);
		}
	}

	/**
	 * ����ת������
	 * @param time_local
	 * @return
	 */
	public static String formatDate(String time_local) {
		try {
			//��Դ���ݸ�ʽת��ΪĿ���ʽ
			//dd/MMM/yyyy:HH:mm:ss ת��Ϊ ��ʽ yyyy-MM-dd HH:mm:ss
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			e.printStackTrace();
			//����������������Null
			return null;
		}
	}
	
	
}
