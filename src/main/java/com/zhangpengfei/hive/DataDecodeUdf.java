package com.zhangpengfei.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * add jar /data1/tydic/java_bigdata.jar;
 * create temporary function data_decode as 'com.zhangpengfei.hive.DataDecodeUdf';
 * select response_param from api_call_log_test1 where month_id = 201911 limit 1;
 * select data_decode(response_param) from api_call_log_test1 where month_id = 201911 limit 1;
 *
 * @author 张朋飞
 */
public class DataDecodeUdf extends UDF {

    public Text evaluate(Text str) {

        // 非空校验
        if (null == str || StringUtils.isBlank(str.toString())) {
            return new Text("null");
        }

        // 解析json
        String data;
        try {
            JSONObject jsonObject = new JSONObject(str.toString());
            data = jsonObject.getString("data");
            return new Text(data);
        } catch (JSONException e) {
            e.printStackTrace();
            // 如果解析异常，则直接返回该字段
            return str;
        }
    }


    public static void main(String[] args) {

        DataDecodeUdf demo1 = new DataDecodeUdf();
        System.out.println(demo1.evaluate(new Text("{}")));
    }

}
