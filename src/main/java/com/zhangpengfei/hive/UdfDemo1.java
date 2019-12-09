package com.zhangpengfei.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author 张朋飞
 */
public class UdfDemo1 extends UDF {

    public Text evaluate(Text str) {

        // 非空校验
        if (null == str) {
            return null;
        }
        if (StringUtils.isBlank(str.toString())) {
            return null;
        }

        // 解析json
        JSONObject jsonObject = new JSONObject(str);
        String data = "";
        try {
            data = jsonObject.getString("data");

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return new Text(data);
    }

    public static void main(String[] args) {
        UdfDemo1 demo1 = new UdfDemo1();
        System.out.println(demo1);
    }
}
