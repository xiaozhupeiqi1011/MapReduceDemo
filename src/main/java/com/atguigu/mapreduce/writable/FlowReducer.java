package com.atguigu.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean outV=new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1.遍历集合累加值
        long uptotal=0;
        long downtotal=0;
        for (FlowBean value : values) {
            uptotal+=value.getUpFlow();
            downtotal+=value.getDownFlow();
        }
        //2.封装outK,outV
        outV.setUpFlow(uptotal);
        outV.setDownFlow(downtotal);
        outV.setSumFlow();

        //3.写出
        context.write(key,outV);
    }
}
