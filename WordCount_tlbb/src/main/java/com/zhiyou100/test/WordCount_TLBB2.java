package com.zhiyou100.test;
import java.io.IOException;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;  
  
public class WordCount_TLBB2 {  
      
    public static class SortIntValueMapper extends Mapper<LongWritable, Text, IntWritable, Text>{  
          
        private final static IntWritable wordCount = new IntWritable(1);  
        private Text word = new Text();  
          
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
            StringTokenizer tokenizer = new StringTokenizer(value.toString());  
            while (tokenizer.hasMoreTokens()) {  
                word.set(tokenizer.nextToken().trim());  
                wordCount.set(Integer.valueOf(tokenizer.nextToken().trim()));  
                context.write(wordCount, word);//<k,v>互换  
            }  
        }  
    }  
      
    public static class SortIntValueReduce extends  Reducer<IntWritable, Text, Text, IntWritable> {  
        private Text result = new Text();  
          
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
            for (Text val : values) {  
                result.set(val.toString());  
                context.write(result, key);//<k,v>互换  
            }  
        }  
    }  
      
    public static void main(String[] args) throws Exception {  
          
        Configuration conf = new Configuration();  
        conf.set("fs.defaultFS", "hdfs://master:9000");
	FileSystem fSystem =FileSystem.get(conf);

		Job job = Job.getInstance(conf, "word count");  
        job.setJarByClass(WordCount_TLBB2.class);  
          
        job.setMapperClass(SortIntValueMapper.class);  
        job.setReducerClass(SortIntValueReduce.class);  
          
        job.setOutputKeyClass(IntWritable.class);  
        job.setOutputValueClass(Text.class);  
      // 设置需要计算的数据的保存路径
     			Path inputPath = new Path("hdfs://master:9000/tlbb3/part-r-00000");
     			FileInputFormat.addInputPath(job, inputPath);

     			// 设置计算结果保存的文件夹，一定确保文件夹不存在
     			Path outputDir = new Path("hdfs://master:9000/tlbb4");
			//如果文件夹存在,就删除
			fSystem.delete(outputDir, true);
     			FileOutputFormat.setOutputPath(job, outputDir);
          System.out.println("----------");
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
}  