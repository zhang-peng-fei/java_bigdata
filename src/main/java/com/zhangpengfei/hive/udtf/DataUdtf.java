package com.zhangpengfei.hive.udtf;

import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 *
 *

 add jar /data1/tydic/java_bigdata.jar;
 create temporary function data_decode_udtf as 'com.zhangpengfei.hive.udtf.DataUdtf';
 select data_decode_udtf(1,2,3);

 +------+--+
 | col  |
 +------+--+
 | 1    |
 | 2    |
 | 3    |
 +------+--+

 select data_decode_udtf(map(1,2,3,4));

 +------+--------+--+
 | key  | value  |
 +------+--------+--+
 | 1    | 2      |
 | 3    | 4      |
 +------+--------+--+


 *
 *
 *
 *
 *
 * @author 张朋飞
 */
public class DataUdtf extends GenericUDTF {

    private transient ObjectInspector inputOI = null;

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //得到结构体的字段
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        ObjectInspector[] udfInputOIs = new ObjectInspector[inputFields.size()];
        for (int i = 0; i < inputFields.size(); i++) {
            //字段类型
            udfInputOIs[i] = inputFields.get(i).getFieldObjectInspector();
        }

        if (udfInputOIs.length != 1) {
            throw new UDFArgumentLengthException("explode() takes only one argument");
        }

        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        switch (udfInputOIs[0].getCategory()) {
            case LIST:
                inputOI = udfInputOIs[0];
                //指定list生成的列名，可在as后覆写
                fieldNames.add("col");
                //获取list元素的类型
                fieldOIs.add(((ListObjectInspector) inputOI).getListElementObjectInspector());
                break;
            case MAP:
                inputOI = udfInputOIs[0];
                //指定map中key的生成的列名，可在as后覆写
                fieldNames.add("key");
                //指定map中value的生成的列名，可在as后覆写
                fieldNames.add("value");
                //得到map中key的类型
                fieldOIs.add(((MapObjectInspector) inputOI).getMapKeyObjectInspector());
                //得到map中value的类型
                fieldOIs.add(((MapObjectInspector) inputOI).getMapValueObjectInspector());
                break;
            default:
                throw new UDFArgumentException("explode() takes an array or a map as a parameter");
        }
        //创建一个Struct类型返回
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 输出list
     */
    private transient Object[] forwardListObj = new Object[1];
    /**
     * 输出map
     */
    private transient Object[] forwardMapObj = new Object[2];

    @Override
    public void process(Object[] args) throws HiveException {
        switch (inputOI.getCategory()) {
            case LIST:
                ListObjectInspector listOI = (ListObjectInspector) inputOI;
                List<?> list = listOI.getList(args[0]);
                if (list == null) {
                    return;
                }

                //list中每个元素输出一行
                for (Object o : list) {
                    forwardListObj[0] = o;
                    forward(forwardListObj);
                }
                break;
            case MAP:
                MapObjectInspector mapOI = (MapObjectInspector) inputOI;
                Map<?, ?> map = mapOI.getMap(args[0]);
                if (map == null) {
                    return;
                }
                //map中每一对输出一行
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    forwardMapObj[0] = entry.getKey();
                    forwardMapObj[1] = entry.getValue();
                    forward(forwardMapObj);
                }
                break;
            default:
                throw new TaskExecutionException("explode() can only operate on an array or a map");
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
