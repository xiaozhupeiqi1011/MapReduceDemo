package com.atguigu.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {

    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();
    @Override
    //初始化 order pd
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片信息
        FileSplit split = (FileSplit) context.getInputSplit();
        //通过切片信息获取到输入文件的名称
        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.判断是那个文件的
        if(filename.contains("order")){//处理的是订单表
            String[] split = line.split("\t");
            //封装K,V
            outK.set(split[1]);//K:pid
            outV.setId(split[0]);//id
            outV.setPid(split[1]);//tablebean对象中的pid
            outV.setAmount(Integer.parseInt(split[2]));//amount,并进行了类型的转换
            outV.setPname("");//order表中不包含pname,将其设为默认值""
            outV.setFlag("order");
        }else {//处理的是商品表
            String[] split = line.split("\t");

            //封装K,V
            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);//pd表中不包含amount这个字段，将其设为默认值0
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        //3.写出
        context.write(outK,outV);
    }
}
