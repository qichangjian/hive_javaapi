package com.qcj.custom_function;

import org.apache.hadoop.hive.ql.exec.UDF;

//add jar /home/hadoop1/jar/mapreducetask.jar;   create temporary function myfun as 'com.qcj.hive_udf_define_function.MyUDF2';
//com.qcj.hive_udf_define_function.MyUDF2
public class MyUDF2 extends UDF {
    public int evaluate(String line,String flag){
        int index = 0;
        int count = 0;
        while((index = line.indexOf(flag,index))!=-1){
            index = index + flag.length();
            count ++;
        }
        return count;
    }
}
