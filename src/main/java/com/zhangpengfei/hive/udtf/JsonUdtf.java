package com.zhangpengfei.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 *
 *

 add jar /data1/tydic/java_bigdata.jar;
 create temporary function demo_udtf as 'com.zhangpengfei.hive.udtf.GenericUDTFCount2';
 select demo_udtf(1,2);


 *
 * @author 张朋飞
 */
public class JsonUdtf extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector structObjectInspector) throws UDFArgumentException {



        return super.initialize(structObjectInspector);

    }

    @Override
    public void process(Object[] args) {

    }

    @Override
    public void close() {

    }
}
