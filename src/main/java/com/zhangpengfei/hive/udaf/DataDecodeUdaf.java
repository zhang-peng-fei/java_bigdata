package com.zhangpengfei.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * delete jar /data1/tydic/java_bigdata.jar;
 * select response_param from api_call_log_test1 where month_id=1;
 *
 *+----------------------------------------------------+--+
 * |                   response_param                   |
 * +----------------------------------------------------+--+
 * | {"message":"成功","data":"tfP8RIOAE7W23c8OgECGTklv1u8lhOFT12TTcdmzxKWkYjA/OEwABcmfPVwCz4QNHEOW0KyiJXBInUaWgH8rwG8Hivd1glaFO76rw7wpjhDWa8QzN0IhrLG104q55B3FrLBBvkMgaz+NYW0QCyyhX9FRsnCHrbJ2v8BxICQMSiGUK+IwMQtNiIIWme94nbp3Yt","code":"10000","seqid":"a27aebaa74794f8588bf766e5d6df5a9"} |
 * +----------------------------------------------------+--+
 *
 add jar /data1/tydic/java_bigdata.jar;
 create temporary function data_decode_udaf1 as 'com.zhangpengfei.hive.udaf.DataDecodeUdaf';
 select data_decode_udaf1(response_param) from api_call_log_test1 where month_id=1;
 select data_decode_udaf1(response_param) from api_call_log_test1 where month_id = 201911 and api_type = 1 and result_flag = 1 group by s_id;

 response_param 字段中是一个 json 串，需要解析 json 串，对 code 进行group by 和 sort by



 * * select * from api_call_log_test1 where month_id = 201911 and api_type = 1 limit 5;
 * * select avg(cast(s_id as int)) from api_call_log_test1 where month_id = 201911 and api_type = 1 limit 2;
 * =1371
 *
 * @author 张朋飞
 */
public class DataDecodeUdaf extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(DataDecodeUdaf.class.getName());

    /**
     * 获取 evaluator 的参数类型。这个函数返回对象而不是类的原因是，对象可能需要一些配置(可以被序列化)。
     * 在这种情况下，类必须实现 Serializable 接口。在执行时，我们将从计划中反序列化对象，并使用它来聚合。
     *
     * @param info 用于被调用的 UDAF 的参数信息。
     * @return 如果类没有实现 Serializable，那么我们将在执行时创建该类的新实例。将会调用 GenericUDAFResolver2 接口中的 getEvaluator 方法。
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        ObjectInspector[] inspectors = info.getParameterObjectInspectors();
        if (inspectors.length == 0) {
            throw new UDFArgumentTypeException(inspectors.length - 1, "请指定至少一个参数。");
        }
        // 验证第一个参数，它是要计算的表达式

        if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "只接受原始类型参数，但是" + inspectors[0].getTypeName() + "作为第一个参数传进来");
        }
        return new DataDecodeEvaluator();
    }

    /**
     * 获取 evaluator 的参数类型。这个函数返回对象而不是类的原因是，对象可能需要一些配置(可以被序列化)。
     * 在这种情况下，类必须实现 Serializable 接口。在执行时，我们将从计划中反序列化对象，并使用它来聚合。
     *
     * @param info 参数的类型。我们需要类型信息来知道使用哪个 evaluator 。
     * @return 如果类没有实现 Serializable，那么我们将在执行时创建该类的新实例。将会调用 GenericUDAFResolver 接口中的 getEvaluator 方法
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length > 1) {
            throw new UDFArgumentLengthException("Exactly one argument is expected");
        }
        return new DataDecodeEvaluator();
    }

    public static class DataDecodeEvaluator extends GenericUDAFEvaluator {


        /**
         * 合并结果的类型
         */

        private ArrayList<String> fieldNames;
        private ArrayList<ObjectInspector> fieldValues;
        private ArrayList<String> fieldCommits;

        StandardStructObjectInspector standardStructObjectInspector;

        private StringObjectInspector soi;
        private WritableStringObjectInspector wsoi;
        public DoubleWritable resultSoi;

        public DataDecodeEvaluator() {
        }


        /**
         * @param m
         * @param parameters
         * @return 由 Hive 调用以初始化 UDAF Evaluator 类的实例。
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            this.wsoi = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            this.resultSoi = new DoubleWritable(0);
            LOG.warn("=====================init 方法执行=====================");

            fieldNames = new ArrayList<>();
            fieldValues = new ArrayList<>();
            fieldCommits = new ArrayList<>();

            standardStructObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldValues, fieldCommits);

            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
//            return standardStructObjectInspector;
        }

        /**
         * 定义一个AbstractAggregationBuffer类来缓存合并值
         */
        static class CodeAgg extends AbstractAggregationBuffer {
            Map<String, Integer> resMap;

            @Override
            public String toString() {
                return "CodeAgg{" +
                        "resMap=" + resMap +
                        '}';
            }

        }

        /**
         * @return 返回一个将用于存储临时聚合结果的对象。
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() {
            CodeAgg codeAgg = new CodeAgg();
            codeAgg.resMap = new HashMap<>(16);
            reset(codeAgg);
            return codeAgg;

        }

        /**
         * 重置 aggregation。
         * 复用  aggregation
         *
         * @param aggregationBuffer
         */
        @Override
        public void reset(AggregationBuffer aggregationBuffer) {
            CodeAgg codeAgg = (CodeAgg) aggregationBuffer;
            codeAgg.resMap.clear();
        }

        /**
         * 遍历原始数据
         * 在聚合缓冲区中处理新数据行
         *
         * @param aggregationBuffer
         * @param objects
         */
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) {
            Object object = objects[0];
            LOG.error("iterate=" + object);
            try {
                JSONObject jsonObject = new JSONObject(object.toString());
                String code = jsonObject.getString("code");
                merge(aggregationBuffer,code);
            } catch (JSONException e) {
                LOG.error(e.getMessage());
                return;
            }
            LOG.warn("=====================iterator 方法执行=====================");
        }

        /**
         * 得到部分聚合结果
         *
         * @param aggregationBuffer
         * @return
         */
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) {
            return terminate(aggregationBuffer);
        }

        /**
         * 与部分聚合结果合并。
         * 注意：如果没有输入数据，可能会 null
         *
         * @param aggregationBuffer
         * @param partial
         */
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) {
            if (partial != null) {
                CodeAgg codeAgg = (CodeAgg) aggregationBuffer;
                Map<String, Integer> map = codeAgg.resMap;
                String codeN = partial.toString();

                if (map.containsKey(codeN)) {
                    map.put(codeN, map.get(codeN) + 1);
                } else {
                    map.put(codeN, 1);
                }

            }
        }

        /**
         * 获得最终的聚合结果返回到 Hive
         *
         * @param aggregationBuffer
         * @return
         */
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) {
            CodeAgg codeAgg = (CodeAgg) aggregationBuffer;
            LOG.warn("=====================terminate 方法执行，说明聚合结束=====================");
            return codeAgg.resMap;
//            return standardStructObjectInspector.setStructFieldData(Map, codeAgg.resMap.)
        }

    }

    public static void main(String[] args) {

    }
}
