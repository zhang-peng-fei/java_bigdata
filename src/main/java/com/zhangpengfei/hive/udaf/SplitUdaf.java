package com.zhangpengfei.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

/**
 * 对 demo1 表中的 name 字段做切分，然后聚合之后的值应该为4
 *
 * select * from demo1;
 * +-----------+------------------+------------+--+
 * | demo1.id  |    demo1.name    | demo1.age  |
 * +-----------+------------------+------------+--+
 * | 1         | fred flintstone  | 35         |
 * | 2         | barney rubble    | 32         |
 * +-----------+------------------+------------+--+
 *
 add jar /data1/tydic/java_bigdata.jar;
 create temporary function split_udaf as 'com.zhangpengfei.hive.udaf.SplitUdaf';
 select split_udaf(name) from demo1;

 +------+--+
 | _c0  |
 +------+--+
 | 4    |
 +------+--+

 *
 *
 * @author 张朋飞
 */
public class SplitUdaf extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(SplitUdaf.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) {
        // Type-checking goes here!

        return new SplitUdafEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) {
        return new SplitUdafEvaluator();
    }

    public static class SplitUdafEvaluator extends GenericUDAFEvaluator {


        /**
         *
         * @param m
         * @param parameters
         * @return 定义返回值的类型为：IntWritable
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }

        /**
         * 初始化一个容器，用来存放中间计算结果数据
         * @return
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() {
            CountAgg countAgg = new CountAgg();
            reset(countAgg);
            return countAgg;
        }

        /**
         * 单节点计算完成之后，重置容器的值为0
         * @param agg
         */
        @Override
        public void reset(AggregationBuffer agg) {
            CountAgg countAgg = (CountAgg) agg;
            countAgg.value = 0;
        }

        /**
         * 遍历每个 dataset，最单个串切分，然后通过调用 merge 函数累加 length
         * @param agg
         * @param parameters
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) {
            String str = parameters[0].toString();
            LOG.warn(str);
            String[] split = str.split(" ");
            CountAgg count = (CountAgg) agg;
            merge(count,split.length);
        }

        /**
         * 以可持久化的方式返回当前聚合的内容，此处直接调用 terminate 方法
         * @param agg
         * @return
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) {
            return terminate(agg);
        }

        /**
         * 将 terminatePartial 返回的部分聚合合并到当前聚合中
         * @param agg
         * @param partial
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) {
            if (null != partial) {
                CountAgg count = (CountAgg) agg;
                count.value += Integer.parseInt(partial.toString());
            }
        }

        /**
         * 将聚合的最终结果返回到Hive
         * 注意：该方法的返回类型必须与 init 方法的返回类型一直，不然会出现类型转换异常
         * @param agg
         * @return
         */
        @Override
        public Object terminate(AggregationBuffer agg) {
            CountAgg res = (CountAgg) agg;
            IntWritable intWritable = new IntWritable();
            intWritable.set(res.value);
            return intWritable;
        }
        // UDAF logic goes here!

        /**
         * 容器类
         */
        class CountAgg extends GenericUDAFEvaluator.AbstractAggregationBuffer {
            int value;

            @Override
            public int estimate() {
                return JavaDataModel.PRIMITIVES1;
            }
        }
    }
}
