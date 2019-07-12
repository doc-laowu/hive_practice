package com.vhall.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Title: IpUdtf
 * @ProjectName IpUdtf
 * @Description: TODO 通过ip获取地理位置信息
 * @Author yisheng.wu
 * @Date 2019/7/815:45
 */
public class IpUdtf extends GenericUDTF {

    //返回的结果的数组
    private Object[] forwardObj = new Object[3];

    //redis连接池的信息
    Properties redisPro;

    // 调用udtf函数返回的值
    private PrimitiveObjectInspector stringOI1 = null;
    private PrimitiveObjectInspector stringOI2 = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        if (argOIs.length != 2) {
            throw new UDFArgumentException("NameParserGenericUDTF() takes exactly one argument");
        }

        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && !argOIs[0].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
            throw new UDFArgumentException("NameParserGenericUDTF() the first paramter expect string");
        }

        if(argOIs[1].getCategory() != ObjectInspector.Category.PRIMITIVE
                && !argOIs[1].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)){
            throw new UDFArgumentException("NameParserGenericUDTF() the second paramter expect string");
        }

        // 输入格式 inspectors
        stringOI1 = (PrimitiveObjectInspector)argOIs[0];
        stringOI2 = (PrimitiveObjectInspector)argOIs[1];

        // 输出字段名
        ArrayList<String> fieldNames = new ArrayList<String>(3);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(3);

        fieldNames.add("country");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("region");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("city");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
      * @Author: yisheng.wu
      * @Description TODO 判断是否是合法的ip
      * @Date 10:43 2019/7/9
      * @Param [ip]
      * @return boolean
      **/
    public boolean isIP(String ip){
        Pattern ipPattern=Pattern.compile("([1-9]|[1-9]\\d|1\\d{2}|2[0-1]\\d|22[0-3])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}");
        Matcher matcher=ipPattern.matcher(ip);
        return matcher.matches();
    }

    /**
      * @Author: yisheng.wu
      * @Description TODO 数据业务逻辑处理类
      * @Date 16:44 2019/7/8
      * @Param [APP_log_data]
      * @return java.util.ArrayList<java.lang.Object[]>
      **/
    public void processInputRecord(String ip_str) throws Exception {

        //要获取的结果的值
        String country;
        String region;
        String city;

        Jedis ip_rs = null;
        Object ipSet[] = null;

        ip_rs = SingleJedisPool.getJedis(redisPro);

        if (null != ip_str && isIP(ip_str)) {
            ipSet = ip_rs.zrevrangeByScore("ip_sorted_set",new IpUtil().ipToLong(ip_str),0,0,1).toArray();

            if(ipSet != null && ipSet.length > 0){

                String ip_info = ipSet[0].toString();
                String[] str_arr = ip_info.split("@");
                country = "*".equals(str_arr[1]) ?  "其他" : str_arr[1];
                region = "*".equals(str_arr[2]) ? "其他" : str_arr[2];
                city = "*".equals(str_arr[3]) ? str_arr[2] : str_arr[3];
            }else{

                country = "其他";
                region = "其他";
                city = "其他";
            }

        }else {

            country = "其他";
            region = "其他";
            city = "其他";
        }

        forwardObj[0] = country;
        forwardObj[1] = region;
        forwardObj[2] = city;

        ip_rs.close();

        //将结果返回出去
        forward(forwardObj);

    }

    /**
      * @Author: yisheng.wu
      * @Description TODO 实际数据处理的函数
      * @Date 18:09 2019/7/8
      * @Param [args] TODO 第一个是传入的数据 第二个为传入的redis配置文件位置
      * @return void
      **/
    @Override
    public void process(Object[] args) {

        try {
            //传入的ip地址
            String ip_str =  stringOI1.getPrimitiveJavaObject(args[0]).toString();
            //传入的配置文件位置
            String path = stringOI2.getPrimitiveJavaObject(args[1]).toString();
            //初始化redis连接的属性
            redisPro = new PropsUtil().getRedisProps(path);
            processInputRecord(ip_str);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
      * @Author: yisheng.wu
      * @Description TODO 资源清理，或返回结果
      * @Date 11:31 2019/7/9
      * @Param []
      * @return void
      **/
    @Override
    public void close() throws HiveException {

    }
}
