package com.zhangpengfei.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 将 response_param 中的数据以 json 解析，然后将 data 取出打印
 * select response_param from api_call_log_test1 where month_id = 201911 limit 1;
 *
 * +----------------------------------------------------+--+
 * |                   response_param                   |
 * +----------------------------------------------------+--+
 * | {"message":"成功","data":"tfP8RIOAE7W23c8OgECGTklv1u8lhOFT12TTcdmzxKWkYjA/OEwABcmfPVwCz4QNHEOW0KyiJXBInUaWgH8rwG8Hivd1glaFO76rw7wpjhDWa8QzN0IhrLG104q55B3FrLBBvkMgaz+NYW0QCyyhX9FRsnCHrbJ2v8BxICQMSiGUK+IwMQtNiIIWme94nbp3Yt","code":"10000","seqid":"a27aebaa74794f8588bf766e5d6df5a9"} |
 * +----------------------------------------------------+--+
 *
 add jar /data1/tydic/java_bigdata.jar;
 create temporary function data_decode as 'com.zhangpengfei.hive.DataDecodeUdf';
 select data_decode(response_param) from api_call_log_test1 where month_id = 201911 limit 1;

 +----------------------------------------------------+--+
 |                        _c0                         |
 +----------------------------------------------------+--+
 | tfP8RIOAE7W23c8OgECGTklv1u8lhOFT12TTcdmzxKWkYjA/OEwABcmfPVwCz4QNHEOW0KyiJXBInUaWgH8rwG8Hivd1glaFO76rw7wpjhDWa8QzN0IhrLG104q55B3FrLBBvkMgaz+NYW0QCyyhX9FRsnCHrbJ2v8BxICQMSiGUK+IwMQtNiIIWme94nbp3Yt |
 +----------------------------------------------------+--+


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
