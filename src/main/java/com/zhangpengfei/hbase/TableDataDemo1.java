package com.zhangpengfei.hbase;

import com.zhangpengfei.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
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

//            insertData(table);
//            selectDataByRowKey(table);
//            deleteData(table);
            scanData(table);
        }
    }

    private static void deleteData(HTable table) throws IOException {
        Delete delete = new Delete("rowKey1".getBytes());
        // 指定 列族 名称，删除整个列族
        delete.addFamily("f1".getBytes());
        // 指定列族下的一个列，删除列
        delete.addColumn("f1".getBytes(), "key1".getBytes());
        // 什么也不指定，则删除整个 rowKey 下的数
        table.delete(delete);
    }

    private static void scanData(HTable table) throws IOException {
        // 初始化 startRow 和 endRow
        Scan scan = new Scan("123".getBytes(), "rowKey11".getBytes());

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            // 获取 rowKey
            String rowKey = new String(result.getRow());
            // 获取 key
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap("f1".getBytes());
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                String key = new String(entry.getKey());
                String value = new String(entry.getValue());
                // 打印结果
                System.err.println("rowKey is : " + rowKey + ", key is : " + key + ", value is : " + value);
            }
        }
    }

    private static void selectDataByRowKey(HTable table) throws IOException {
        Get get = new Get("rowKey1".getBytes());
        // 获取单个 rowKey 下整个结果
        Result result = table.get(get);
        System.err.println("result is : " + result.toString());
        // 根据列族，获取 key
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap("f1".getBytes());
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            System.err.println("key is : " + new String(entry.getKey()) + ", value is : " + new String(entry.getValue()));
        }
        // 获取 value
        byte[] value = result.getValue("f1".getBytes(), "key1".getBytes());
        System.err.println("value is : " + new String(value));
    }

    private static void insertData(HTable table) throws IOException {
        // rowKey 前缀
        String rowKey = "rowKey";
        // 列族名称
        String family = "f1";
        // 造一批假数据
        for (int i = 0; i < 50; i++) {
            Put put = new Put(Bytes.toBytes(rowKey + i));
            for (int j = 0; j < 10; j++) {
                put.add(family.getBytes(), Bytes.toBytes(rowKey + i + "-key" + j), Bytes.toBytes(rowKey + i + "-value" + j));
            }
            table.put(put);
        }
    }

}
