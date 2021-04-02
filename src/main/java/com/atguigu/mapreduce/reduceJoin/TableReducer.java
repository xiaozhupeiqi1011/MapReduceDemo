package com.atguigu.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    //每个key调用一次reduce方法
    //在此案例中一共只有3个key，且相同的key中有三条记录，其中有两条是order表的记录，有一条是pd表的记录
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //准备两个集合，用于存储相同key的order表和pd表的信息记录
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        //循环遍历
        for (TableBean value : values) {

            if ("order".equals(value.getFlag())){//订单表
                TableBean tmptableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmptableBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tmptableBean);
            }else {//商品表
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        //循环遍历orderBeans,赋值pdname
        for (TableBean orderBean : orderBeans) {

            orderBean.setPname(pdBean.getPname());
            context.write(orderBean,NullWritable.get());
        }
    }
}
