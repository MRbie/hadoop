package com.bie.dataStream.hive.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

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

import com.bie.dataStream.hive.bean.PageViewsBean;
import com.bie.dataStream.hive.bean.VisitBean;

/** 
* @author  Author:������ 
* @date Date:2017��12��16�� ����4:43:14 
* 
* 1:��pageviewsģ�ͽ�������н�һ�������visitģ��
*	 sessionid  start-time   out-time   start-page   out-page   pagecounts  ......
* 
*/
public class ClickStreamVisit {

	// ��session��Ϊkey���������ݵ�reducer
	static class ClickStreamVisitMapper extends Mapper<LongWritable, Text, Text, PageViewsBean> {

		PageViewsBean pvBean = new PageViewsBean();
		Text k = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = line.split("\001");
			int step = Integer.parseInt(fields[5]);
			//(String session, String remote_addr, String timestr, String request, int step, String staylong, String referal, String useragent, String bytes_send, String status)
			//299d6b78-9571-4fa9-bcc2-f2567c46df3472.46.128.140-2013-09-18 07:58:50/hadoop-zookeeper-intro/160"https://www.google.com/""Mozilla/5.0"14722200
			pvBean.set(fields[0], fields[1], fields[2], fields[3],fields[4], step, fields[6], fields[7], fields[8], fields[9]);
			k.set(pvBean.getSession());
			context.write(k, pvBean);

		}

	}

	//
	static class ClickStreamVisitReducer extends Reducer<Text, PageViewsBean, NullWritable, VisitBean> {

		@Override
		protected void reduce(Text session, Iterable<PageViewsBean> pvBeans, Context context) throws IOException, InterruptedException {

			// ��pvBeans����step����
			ArrayList<PageViewsBean> pvBeansList = new ArrayList<PageViewsBean>();
			for (PageViewsBean pvBean : pvBeans) {
				PageViewsBean bean = new PageViewsBean();
				try {
					BeanUtils.copyProperties(bean, pvBean);
					pvBeansList.add(bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			Collections.sort(pvBeansList, new Comparator<PageViewsBean>() {

				@Override
				public int compare(PageViewsBean o1, PageViewsBean o2) {

					return o1.getStep() > o2.getStep() ? 1 : -1;
				}
			});

			// ȡ���visit����βpageview��¼�������ݷ���VisitBean��
			VisitBean visitBean = new VisitBean();
			// ȡvisit���׼�¼
			visitBean.setInPage(pvBeansList.get(0).getRequest());
			visitBean.setInTime(pvBeansList.get(0).getTimestr());
			// ȡvisit��β��¼
			visitBean.setOutPage(pvBeansList.get(pvBeansList.size() - 1).getRequest());
			visitBean.setOutTime(pvBeansList.get(pvBeansList.size() - 1).getTimestr());
			// visit���ʵ�ҳ����
			visitBean.setPageVisits(pvBeansList.size());
			// �����ߵ�ip
			visitBean.setRemote_addr(pvBeansList.get(0).getRemote_addr());
			// ����visit��referal
			visitBean.setReferal(pvBeansList.get(0).getReferal());
			visitBean.setSession(session.toString());

			context.write(NullWritable.get(), visitBean);

		}

	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ClickStreamVisit.class);

		job.setMapperClass(ClickStreamVisitMapper.class);
		job.setReducerClass(ClickStreamVisitReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViewsBean.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VisitBean.class);
		
		
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.setInputPaths(job, new Path("C:/Users/bhlgo/Desktop/pageviews"));
		FileOutputFormat.setOutputPath(job, new Path("C:/Users/bhlgo/Desktop/visitout"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);

	}
	
	
}
