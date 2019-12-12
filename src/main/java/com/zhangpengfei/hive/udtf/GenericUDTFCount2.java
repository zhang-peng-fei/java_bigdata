package com.zhangpengfei.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 *
 *

 add jar /data1/tydic/java_bigdata.jar;
 create temporary function demo_udtf as 'com.zhangpengfei.hive.udtf.GenericUDTFCount2';
 select demo_udtf(1,2);

 调用该函数将返回
 +-------+--+
 | col1  |
 +-------+--+
 | 0     |
 | 0     |
 +-------+--+

 *
 *
 * @author 张朋飞
 */
public class GenericUDTFCount2 extends GenericUDTF {

    Integer count = Integer.valueOf(0);
    Object[] forwardObj = new Object[1];

    @Override
    public void process(Object[] args) throws HiveException {
        forwardObj[0] = count;
        forward(forwardObj);
        forward(forwardObj);
    }

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldObjectInspector = new ArrayList<>();
        ArrayList<String> fieldCommits = new ArrayList<>();

        fieldNames.add("col1");
        fieldObjectInspector.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldCommits.add("第一个列");

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldObjectInspector, fieldCommits);

    }

    @Override
    public void close() throws HiveException {
        count = Integer.valueOf(count.intValue() + 1);
    }
}
