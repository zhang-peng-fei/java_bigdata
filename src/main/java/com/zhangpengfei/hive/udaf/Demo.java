package com.zhangpengfei.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * 相当于 count 函数的实现
 * select * from pageviews;
 *
 * +-------------------+-----------------+----------------------+----------------------+--+
 * | pageviews.userid  | pageviews.link  | pageviews.came_from  | pageviews.datestamp  |
 * +-------------------+-----------------+----------------------+----------------------+--+
 * | tlee              | finance.com     | NULL                 | 2014-09-21           |
 * | tlee              | finance.com     | NULL                 | 2014-09-21           |
 * | jsmith            | mail.com        | sports.com           | 2014-09-23           |
 * | jdoe              | mail.com        | NULL                 | 2014-09-23           |
 * | tjohnson          | sports.com      | finance.com          | 2014-09-23           |
 * | tjohnson          | sports.com      | finance.com          | 2014-09-23           |
 * +-------------------+-----------------+----------------------+----------------------+--+
 *
add jar /data1/tydic/java_bigdata.jar;
create temporary function data_decode_udaf2 as 'com.zhangpengfei.hive.udaf.Demo';
select data_decode_udaf2(distinct userid) from pageviews;
 该函数等价于 count 函数，对 userid 去重，然后 count，结果为4
select data_decode_udaf2(userid) from pageviews;

 select userid, data_decode_udaf2(userid) from pageviews group by userid;



 * @author 张朋飞
 */
@Description(name = "count", value = "_FUNC_(*) - Returns the total number of retrieved rows, including rows containing NULL values.\n_FUNC_(expr) - Returns the number of rows for which the supplied expression is non-NULL.\n_FUNC_(DISTINCT expr[, expr...]) - Returns the number of rows for which the supplied expression(s) are unique and non-NULL.")
public class Demo implements GenericUDAFResolver2 {
    private static final Log LOG;

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) {
        return new GenericUDAFCountEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException {
        TypeInfo[] parameters = paramInfo.getParameters();

        if (parameters.length == 0) {
            if (!(paramInfo.isAllColumns())) {
                throw new UDFArgumentException("Argument expected");
            }
            if ((paramInfo.isDistinct())) {
                throw new AssertionError("DISTINCT not supported with *");
            }
        } else {
            if ((parameters.length > 1) && (!(paramInfo.isDistinct()))) {
                throw new UDFArgumentException("DISTINCT keyword must be specified");
            }
            assert (!(paramInfo.isAllColumns())) : "* not supported in expression list";
        }

        return new GenericUDAFCountEvaluator().setCountAllColumns(paramInfo.isAllColumns());
    }

    static {
        LOG = LogFactory.getLog(Demo.class.getName());
    }

    public static class GenericUDAFCountEvaluator extends GenericUDAFEvaluator {
        private boolean countAllColumns;
        private LongObjectInspector partialCountAggOI;
        private LongWritable result;

        public GenericUDAFCountEvaluator() {
            this.countAllColumns = false;
        }

        @Override
        public ObjectInspector init(GenericUDAFEvaluator.Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            this.partialCountAggOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;

            this.result = new LongWritable(0L);
            LOG.warn("=====================初始化方法执行=====================");
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        private GenericUDAFCountEvaluator setCountAllColumns(boolean countAllCols) {
            this.countAllColumns = countAllCols;
            return this;
        }

        @Override
        public GenericUDAFEvaluator.AggregationBuffer getNewAggregationBuffer() {
            CountAgg buffer = new CountAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(GenericUDAFEvaluator.AggregationBuffer agg) {
            ((CountAgg) agg).value = 0L;
        }

        @Override
        public void iterate(GenericUDAFEvaluator.AggregationBuffer agg, Object[] parameters) {
            if (parameters == null) {
                return;
            }
            if (this.countAllColumns) {
                assert (parameters.length == 0);
                ((CountAgg) agg).value += 1L;
            } else {
                assert (parameters.length > 0);
                boolean countThisRow = true;
                for (Object nextParam : parameters) {
                    if (nextParam == null) {
                        countThisRow = false;
                        break;
                    }
                }
                if (countThisRow) {
                    ((CountAgg) agg).value += 1L;
                }
            }
        }

        @Override
        public void merge(GenericUDAFEvaluator.AggregationBuffer agg, Object partial) {
            if (partial != null) {
                long p = this.partialCountAggOI.get(partial);
                ((CountAgg) agg).value += p;
            }
        }

        @Override
        public Object terminate(GenericUDAFEvaluator.AggregationBuffer agg) {
            this.result.set(((CountAgg) agg).value);
            LOG.warn("=====================terminate 方法执行=====================");
            return this.result;
        }

        @Override
        public Object terminatePartial(GenericUDAFEvaluator.AggregationBuffer agg) {
            return terminate(agg);
        }

        @GenericUDAFEvaluator.AggregationType(estimable = true)
        static class CountAgg extends GenericUDAFEvaluator.AbstractAggregationBuffer {
            long value;

            @Override
            public int estimate() {
                return 8;
            }
        }
    }
    public static void main(String[] args){

    }
}