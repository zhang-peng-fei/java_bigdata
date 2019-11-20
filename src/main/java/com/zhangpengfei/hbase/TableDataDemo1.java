package com.zhangpengfei.hbase;

import com.zhangpengfei.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * @author 张朋飞
 */
public class TableDataDemo1 {
    public static void main(String[] args) throws IOException {
        Configuration hbaseConfig = HbaseUtil.getHbaseConfig();

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Admin admin = connection.getAdmin()) {
            HTable table = new HTable(hbaseConfig, "tb1");

            insertData(table);
            selectData(table);
        }
    }

    private static void selectData(HTable table) throws IOException {
        Get get = new Get("rowKey1".getBytes());
        // 获取单个 rowKey 下整个结果
        Result result = table.get(get);
        System.err.println("result is : " + result.toString());
        // 获取 key
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap("f1".getBytes());
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            System.err.println("key is : " + new String(entry.getKey()) + ", value is : " + new String(entry.getValue()));
        }
        // 获取 value
        byte[] value = result.getValue("f1".getBytes(), "key1".getBytes());
        System.err.println("value is : " + new String(value));
    }

    private static void insertData(HTable table) throws IOException {

        String rowKey = "rowKey";
        String family = "f1";

        for (int i = 0; i < 50; i++) {
            Put put = new Put(Bytes.toBytes(rowKey + i));

            for (int j = 0; j < 10; j++) {
                put.add(family.getBytes(), Bytes.toBytes(rowKey + i + "-key" + j),Bytes.toBytes(rowKey+i + "-value"+j));
            }
            table.put(put);
        }

       /* byte[] CF = "f1".getBytes();
        byte[] ATTR = "rowKey1-key2".getBytes();

        Put put = new Put(Bytes.toBytes("rowKey1"));
        put.add(CF, ATTR, Bytes.toBytes("rowKey1-value2"));
        table.put(put);*/
    }

}
