package com.atguigu.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable,Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.ETL
        Boolean result=parseLog(line,context);
        if (!result){
            return;
        }
        ///3.写出
        context.write(value,NullWritable.get());
    }

    private Boolean parseLog(String line, Context context) {
        //1.切割
        String[] fields = line.split(" ");
        //2.判断日志的长度是否大于11
        if (fields.length>11){
            return true;
        }else {
            return false;
        }
    }
}
