package com.zhangpengfei.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

public class JsonCountUdaf extends AbstractGenericUDAFResolver {

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
        boolean isAllColumns = false;

        if (inspectors.length == 0) {
            if (!info.isAllColumns()) {
                throw new UDFArgumentException("Argument expected");
            }

            if (info.isDistinct()) {
                throw new UDFArgumentException("DISTINCT not supported with");
            }
            isAllColumns = true;
        } else if (inspectors.length != 1) {
            throw new UDFArgumentLengthException("Exactly one argument is expected.");
        }
        return new JsonCountEvaluator(isAllColumns);
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
        return new JsonCountEvaluator();
    }

    public static class JsonCountEvaluator extends GenericUDAFEvaluator {

        private boolean isAllColumns;

        /**
         * 合并结果的类型
         */
        private LongObjectInspector aggOI;

        private LongWritable result;

        public JsonCountEvaluator() {
        }

        public JsonCountEvaluator(boolean isAllColumns) {
            this.isAllColumns = isAllColumns;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            //当是combiner和reduce阶段时，获取合并结果的类型，因为需要执行merge方法
            //merge方法需要部分合并的结果类型来取得值
            if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                aggOI = (LongObjectInspector) parameters[0];
            }

            //保存总结果
            result = new LongWritable(0);
            //局部合并结果的类型和总合并结果的类型都是long
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        /**
         * 定义一个AbstractAggregationBuffer类来缓存合并值
         */
        static class CountAgg extends AbstractAggregationBuffer {
            long value;

            /**
             * 返回类型占的字节数，long为8
             *
             * @return
             */
            @Override
            public int estimate() {
                return JavaDataModel.PRIMITIVES2;
            }
        }

        /**
         * @return Get a new aggregation object.
         * @throws HiveException
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CountAgg countAgg = new CountAgg();
            reset(countAgg);
            return countAgg;
        }

        /**
         * 重置 aggregation。复用  aggregation
         *
         * @param countAgg
         * @throws HiveException
         */
        @Override
        public void reset(AggregationBuffer  countAgg) throws HiveException {
            ((CountAgg)countAgg).value = 0;
        }

        /**
         * 遍历原始数据
         *
         * @param countAgg
         * @param objects
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer  countAgg, Object[] objects) throws HiveException {
            //parameters为输入数据
            //parameters == null means the input table/split is empty
            if (countAgg == null) {
                return;
            }
            if (isAllColumns) {
                ((CountAgg)countAgg).value++;
            } else {
                boolean countThisRow = true;
                for (Object nextParam : objects) {
                    if (nextParam == null) {
                        countThisRow = false;
                        break;
                    }
                }
                if (countThisRow) {
                    ((CountAgg)countAgg).value++;
                }
            }
        }

        /**
         * 得到部分聚合结果
         *
         * @param countAgg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer  countAgg) throws HiveException {
            return terminate(countAgg);
        }

        /**
         * 与部分聚合结果合并。
         * 注意：如果没有输入数据，可能会 null
         *
         * @param countAgg
         * @param partial
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer  countAgg, Object partial) throws HiveException {
            if (partial != null){
                //累加部分聚合的结果
                ((CountAgg)countAgg).value += aggOI.get(partial);
            }
        }

        /**
         * 获得最终的聚合结果
         *
         * @param countAgg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer  countAgg) throws HiveException {
            result.set(((CountAgg)countAgg).value);
            return result;
        }
    }
}
