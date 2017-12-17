package com.bie.dataStream.hive.mapReduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bie.dataStream.hive.bean.WebLogBean;

/** 
* @author  Author:������ 
* @date Date:2017��12��16�� ����4:39:43 
*
* 1:����ϴ֮�����־����������pageviewsģ������
* 2:������������ϴ����Ľ������
* 3:���ֳ�ÿһ�λỰ����ÿһ��visit��session��������session-id�����uuid��
* 4:�����ÿһ�λỰ�������ʵ�ÿ��ҳ�棨����ʱ�䣬url��ͣ��ʱ�����Լ���ҳ�������session�е���ţ�
* 5:����referral_url��body_bytes_send��useragent
* 
*/
public class ClickStream {

	static class ClickStreamMapper extends Mapper<LongWritable, Text, Text, WebLogBean> {

		Text k = new Text();
		WebLogBean v = new WebLogBean();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String[] fields = line.split("\001");
			if (fields.length < 9) return;
			//���зֳ����ĸ��ֶ�set��weblogbean��
			v.set("true".equals(fields[0]) ? true : false, fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);
			//ֻ����Ч��¼�Ž����������
			if (v.isValid()) {
				k.set(v.getRemote_addr());
				context.write(k, v);
			}
		}
	}

	static class ClickStreamReducer extends Reducer<Text, WebLogBean, NullWritable, Text> {

		Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
			ArrayList<WebLogBean> beans = new ArrayList<WebLogBean>();

			// �Ƚ�һ���û������з��ʼ�¼�е�ʱ���ó�������
			try {
				for (WebLogBean bean : values) {
					WebLogBean webLogBean = new WebLogBean();
					try {
						BeanUtils.copyProperties(webLogBean, bean);
					} catch(Exception e) {
						e.printStackTrace();
					}
					beans.add(webLogBean);
				}
				//��bean��ʱ���Ⱥ�˳������
				Collections.sort(beans, new Comparator<WebLogBean>() {

					@Override
					public int compare(WebLogBean o1, WebLogBean o2) {
						try {
							Date d1 = toDate(o1.getTime_local());
							Date d2 = toDate(o2.getTime_local());
							if (d1 == null || d2 == null)
								return 0;
							return d1.compareTo(d2);
						} catch (Exception e) {
							e.printStackTrace();
							return 0;
						}
					}

				});

				/**
				 * �����߼�Ϊ��������bean�зֱ������visit������һ��visit�������ʵ�page��˳����step
				 */
				
				int step = 1;
				String session = UUID.randomUUID().toString();
				for (int i = 0; i < beans.size(); i++) {
					WebLogBean bean = beans.get(i);
					// �������1�����ݣ���ֱ�����
					if (1 == beans.size()) {
						
						// ����Ĭ��ͣ���г�Ϊ60s
						v.set(session+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001"
								+ bean.getStatus());
						context.write(NullWritable.get(), v);
						session = UUID.randomUUID().toString();
						break;
					}

					// �����ֹ1�����ݣ��򽫵�һ������������������ڶ���ʱ�����
					if (i == 0) {
						continue;
					}

					// �������ʱ���
					long timeDiff = timeDiff(toDate(bean.getTime_local()), toDate(beans.get(i - 1).getTime_local()));
					// �������-�ϴ�ʱ���<30���ӣ������ǰһ�ε�ҳ�������Ϣ
					
					if (timeDiff < 30 * 60 * 1000) {
						
						v.set(session+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + step + "\001" + (timeDiff / 1000) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
								+ beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
						context.write(NullWritable.get(), v);
						step++;
					} else {
						
						// �������-�ϴ�ʱ���>30���ӣ������ǰһ�ε�ҳ�������Ϣ�ҽ�step���ã��Էָ�Ϊ�µ�visit
						v.set(session+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + (step) + "\001" + (60) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
								+ beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
						context.write(NullWritable.get(), v);
						// �������һ��֮������step���
						step = 1;
						session = UUID.randomUUID().toString();
					}

					// ����˴α����������һ�����򽫱���ֱ�����
					if (i == beans.size() - 1) {
						// ����Ĭ��ͣ���г�Ϊ60s
						v.set(session+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001" + bean.getStatus());
						context.write(NullWritable.get(), v);
					}
				}

			} catch (ParseException e) {
				e.printStackTrace();

			}

		}

		private String toStr(Date date) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			return df.format(date);
		}

		private Date toDate(String timeStr) throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			return df.parse(timeStr);
		}

		private long timeDiff(String time1, String time2) throws ParseException {
			Date d1 = toDate(time1);
			Date d2 = toDate(time2);
			return d1.getTime() - d2.getTime();

		}

		private long timeDiff(Date time1, Date time2) throws ParseException {

			return time1.getTime() - time2.getTime();

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ClickStream.class);

		job.setMapperClass(ClickStreamMapper.class);
		job.setReducerClass(ClickStreamReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WebLogBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileInputFormat.setInputPaths(job, new Path("C:/Users/bhlgo/Desktop/output"));
		FileOutputFormat.setOutputPath(job, new Path("C:/Users/bhlgo/Desktop/pageviews"));

		job.waitForCompletion(true);

	}
}
