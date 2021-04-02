package com.atguigu.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
     public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        //1.获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.设置jar包路径
        job.setJarByClass(MapJoinDriver.class);
        //3.关联mapper
        job.setMapperClass(MapJoinMapper.class);
        //4.设置map输出的K,V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5.设置最终输出的K,V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //6.加载缓存数据
        job.addCacheFile(new URI("D:\\百度网盘\\hadoop\\资料\\11_input\\tablecache"));
        //7.不需要Reduce阶段，设置RuduceTask为0
        job.setNumReduceTasks(0);
        //8.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\百度网盘\\hadoop\\资料\\11_input\\inputtable2"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\MapReduceOutput\\output11"));
        //9.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
