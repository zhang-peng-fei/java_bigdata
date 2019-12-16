package com.zhangpengfei.hive.udtf;

import com.zhangpengfei.hive.udaf.Demo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 将 response_param 的 json 字符串纵向打印
 *
 select response_param from api_call_log_test1 where month_id=1;

 +----------------------------------------------------+--+
 |                   response_param                   |
 +----------------------------------------------------+--+
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"5f2ce5642c5b462da8e28f732c954d23"} |
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"3dffc673787141788db02fd57eddfa89"} |
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"309b8a0321064412b6a7c41afb73d08c"} |
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"e0045f27f1e448b7b180fc3ad31d4303"} |
 | {"message":"成功","data":"W0TGLuYhmDinmoMKAIRFDg==","code":"10000","seqid":"c2c910cd36d842f3b815ed346d4208c3"} |
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"ee06a05766a94cb7a943e6e4e15159c2"} |
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"defe7d3d5e2c4e4bbf57d74adaedda24"} |
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"70a0733f96014a1b98fe00e0fdb58d66"} |
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"6bf00f8de72547fdbf74a8a9c9f02cf6"} |
 | {"message":"成功","data":"1oyHcwW1ApLZvfmgnSCGWg==","code":"10000","seqid":"a9f7f7ddc8fc43959dba981a26fe4923"} |
 +----------------------------------------------------+--+

 add jar /data1/tydic/java_bigdata.jar;
 create temporary function demo_udtf_json as 'com.zhangpengfei.hive.udtf.JsonUdtf';
 select demo_udtf_json(response_param) from api_call_log_test1 where month_id=1;

 +----------+--------+---------------------------+-----------------------------------+--+
 | message  |  data  |           code            |               seqid               |
 +----------+--------+---------------------------+-----------------------------------+--+
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | 5f2ce5642c5b462da8e28f732c954d23  |
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | 3dffc673787141788db02fd57eddfa89  |
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | 309b8a0321064412b6a7c41afb73d08c  |
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | e0045f27f1e448b7b180fc3ad31d4303  |
 | 成功       | 10000  | W0TGLuYhmDinmoMKAIRFDg==  | c2c910cd36d842f3b815ed346d4208c3  |
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | ee06a05766a94cb7a943e6e4e15159c2  |
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | defe7d3d5e2c4e4bbf57d74adaedda24  |
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | 70a0733f96014a1b98fe00e0fdb58d66  |
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | 6bf00f8de72547fdbf74a8a9c9f02cf6  |
 | 成功       | 10000  | 1oyHcwW1ApLZvfmgnSCGWg==  | a9f7f7ddc8fc43959dba981a26fe4923  |
 +----------+--------+---------------------------+-----------------------------------+--+

 *
 * @author 张朋飞
 */
public class JsonUdtf extends GenericUDTF {

    private static final Log LOG= LogFactory.getLog(Demo.class.getName());


    @Override
    public StructObjectInspector initialize(StructObjectInspector structObjectInspector) throws UDFArgumentException {


        List<? extends StructField> allStructFieldRefs = structObjectInspector.getAllStructFieldRefs();

        if (allStructFieldRefs.size() != 1) {
            new UDFArgumentLengthException("只需要传入一个参数");
        }

        for (StructField allStructFieldRef : allStructFieldRefs) {

        }

        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("message");
        fieldNames.add("data");
        fieldNames.add("code");
        fieldNames.add("seqid");
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        List<String> fieldCommits = new ArrayList<>();
        fieldCommits.add("消息");
        fieldCommits.add("数据");
        fieldCommits.add("编码");
        fieldCommits.add("序列号");

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs, fieldCommits);

    }

    @Override
    public void process(Object[] args) {
        Object arg = args[0];
        for (Object o : args) {
            String str = o.toString();
            try {
                JSONObject jsonObject = new JSONObject(str);

                String message = jsonObject.getString("message");
                int code = jsonObject.getInt("code");
                String data = jsonObject.getString("data");
                String seqid = jsonObject.getString("seqid");

                Object[] objects = new Object[4];
                objects[0] = message;
                objects[1] = code;
                objects[2] = data;
                objects[3] = seqid;

                try {
                    forward(objects);
                } catch (HiveException e) {
                    e.printStackTrace();
                }
            } catch (JSONException e) {
                LOG.error("不是json，" + e.getMessage());
            }
        }
    }

    @Override
    public void close() {

    }
}
