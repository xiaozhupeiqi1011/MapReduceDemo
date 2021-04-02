package com.atguigu.mapreduce.partitioner1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包
        job.setJarByClass(FlowDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4.设置mapper输出的k,v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.设置最终输出的k,v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //关联自定义的partitioner类
        job.setPartitionerClass(ProvincePartitioner.class);
        //设置ReduceTask的个数
        job.setNumReduceTasks(5);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\百度网盘\\hadoop\\资料\\11_input\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\MapReduceOutput\\output4"));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
