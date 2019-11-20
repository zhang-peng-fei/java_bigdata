package com.zhangpengfei.storm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

/**
 * @author 张朋飞
 */
public class InfoPrintFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {

        Fields fields = tuple.getFields();

        for (String field : fields) {
            String val = tuple.getStringByField(field);
            System.out.println("name:" + field + ",age:" + val);
        }
        return true;
    }
}
