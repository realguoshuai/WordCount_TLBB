
package com.zhiyou100.test;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import org.wltea.analyzer.core.IKSegmenter;

import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;

import java.io.StringReader;

public class WordCount_TLBB {

	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			IKSegmenter iks = new IKSegmenter(new StringReader(value.toString()), true);

			Lexeme t;

			while ((t = iks.next()) != null) {

				word.set(t.getLexemeText());

				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable val : values) {

				sum += val.get();
			}
			result.set(sum);
			
			if (key.toString().length()>1 && key.toString().length()<5) {
				
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(WordCount_TLBB.class);

		job.setMapperClass(WordCountMapper.class);

		job.setCombinerClass(WordCountReducer.class);

		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置需要计算的数据的保存路径
		Path inputPath = new Path("hdfs://master:9000/tlbb.txt");
		FileInputFormat.addInputPath(job, inputPath);

		// 设置计算结果保存的文件夹，一定确保文件夹不存在
		Path outputDir = new Path("hdfs://master:9000/tlbb3");
		FileOutputFormat.setOutputPath(job, outputDir);

		// 提交任务并等待完成，返回值表示任务执行结果
		boolean flag = job.waitForCompletion(true);
		System.out.println("执行完了------------------------");
		// 如果执行成功，退出程序
		System.exit(flag ? 0 : 1);
	}

}
