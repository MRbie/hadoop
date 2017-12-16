package com.bie.dataStream.hive.bean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

/** 
* @author  Author:别先生 
* @date Date:2017年12月16日 下午3:48:28 
*
* 1:解析日志类
* 	过滤js/图片/css等静态资源。
* 
*/
public class WebLogParser {
 
	//时间的格式1和格式2,df1源数据格式，dfs目标格式。
	public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

	/**
	 * 解析每一行数据
	 * @param line
	 * @return 返回WebLogBean
	 */
	public static WebLogBean parser(String line) {
		//创建一个日志实体类
		WebLogBean webLogBean = new WebLogBean();
		//以空格进行截取
		String[] arr = line.split(" ");
		//判断，如果长度大于11的进行截取,否则返回
		if (arr.length > 11) {
			// 记录客户端的ip地址
			webLogBean.setRemote_addr(arr[0]);
			// 记录客户端用户名称,忽略属性"-"
			webLogBean.setRemote_user(arr[1]);
			//获取第三个字段的日期，去掉多余的内容，如[19/Sep/2013:00:31:53 +0000]
			String time_local = formatDate(arr[3].substring(1));
			//判断，如果time_local为null,那么赋予一个默认值
			if(null==time_local){
				time_local="-invalid_time-";
			}
			// 记录访问时间与时区
			webLogBean.setTime_local(time_local);
			// 记录请求的url与http协议
			webLogBean.setRequest(arr[6]);
			// 记录请求状态；成功是200
			webLogBean.setStatus(arr[8]);
			// 记录发送给客户端文件主体内容大小
			webLogBean.setBody_bytes_sent(arr[9]);
			// 用来记录从那个页面链接访问过来的
			webLogBean.setHttp_referer(arr[10]);

			//如果useragent元素较多，拼接useragent
			//如果最后一个字段，长短不一，将其拼接一下
			if (arr.length > 12) {
				//对浏览器字段进行拼接
				StringBuilder sb = new StringBuilder();
				for(int i=11;i<arr.length;i++){
					sb.append(arr[i]);
				}
				// 记录客户浏览器的相关信息
				webLogBean.setHttp_user_agent(sb.toString());
			} else {
				// 记录客户浏览器的相关信息
				webLogBean.setHttp_user_agent(arr[11]);
			}
			// 大于400，HTTP错误
			if (Integer.parseInt(webLogBean.getStatus()) >= 400) {
				webLogBean.setValid(false);
			}
			// 判断，如果time_local为null,判断数据不合法
			if("-invalid_time-".equals(webLogBean.getTime_local())){
				webLogBean.setValid(false);
			}
		} else {
			//false,判断数据不合法,如果不合法直接返回
			webLogBean.setValid(false);
		}
		//将合法或者不合法的数据返回回去。
		return webLogBean;
	}

	/***
	 * 
	 * @param bean
	 * @param pages
	 */
	public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
		//过滤js/图片/css等静态资源
		//如果页面不包含这些，就设置为不合法
		if (!pages.contains(bean.getRequest())) {
			bean.setValid(false);
		}
	}

	/**
	 * 日期转换方法
	 * @param time_local
	 * @return
	 */
	public static String formatDate(String time_local) {
		try {
			//将源数据格式转换为目标格式
			//dd/MMM/yyyy:HH:mm:ss 转换为 格式 yyyy-MM-dd HH:mm:ss
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			e.printStackTrace();
			//解析不出来，返回Null
			return null;
		}
	}
	
	
}
