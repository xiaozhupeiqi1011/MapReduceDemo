package com.atguigu.mapreduce.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN,reduce阶段输入的key类型:Text
 * VALUEIN,map阶段输入的value类型:IntWritable
 * KEYOUT,map阶段输出的key类型:Text
 * VALUEOUT,map阶段输出的value类型:IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        //atguigu,(1,1)
        //1.累加

        for (IntWritable value : values) {
            sum+=value.get();
        }

        //2.写出

        outV.set(sum);
        context.write(key,outV);
    }
}
