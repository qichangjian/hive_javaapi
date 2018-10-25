package com.qcj.custom_function;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 自定义hive  UDF函数
 *
 *
 *   hive:2.3.2
 *  步骤：
 *      导入hive 自定义jar包
 *      继承UDF
 *      实现一个方法，名字必须叫evaluate
 *      com.qcj.hive_udf_define_function.MyUDF
 *
 *      打包完成可能报错
 *      linux下执行命令：
 *      zip -d yourjar.jar 'META-INF/.SF' 'META-INF/.RSA' 'META-INF/*SF'
 */
public class MyUDF extends UDF {
    /**
     *  方法返回值 就是 函数的返回值
     *  方法的参数 就是 函数使用的时候给的参数
     *
     *  注意：
     *  方法public的
     *  返回值不能为void
     */
    //三个数求和
    public int evaluate(int a,int b,int c){
        return a+b+c;
    }

    //ip   192.168.2.1  192.168.002.001
    /**
     * ip_location:ip地址库表  起始ip 终止ip 所属国家 所属省份 所属市
     * log:ip
     *
     */
    public String evaluate(String ip){
        String[] datas = ip.split("\\.");//按.切分转义
        StringBuffer sb = new StringBuffer();
        //前边补三个0
        for(String s:datas){
            s="000"+s;
            s=s.substring(s.length()-3);
            sb.append(s).append(".");
        }
        return sb.substring(0,sb.length()-1);
    }

    /*//本地测试
    public static void main(String[] args) {
        String evaluate = evaluate("2.3.5.3");
        System.out.println(evaluate);
    }*/
}
