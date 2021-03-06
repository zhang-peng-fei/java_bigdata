package com.zhangpengfei.hbase;

import com.zhangpengfei.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;

/**
 * @author 张朋飞
 */
public class TableSchemaDemo1 {

    private static Logger logger = Logger.getLogger(TableSchemaDemo1.class);
    /**
     * 表名
     */
    private static final String TABLE_NAME = "tb1";
    /**
     * 列族名
     */
    private static final String CF_DEFAULT = "f1";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration hbaseConfig = HbaseUtil.getHbaseConfig();

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Admin admin = connection.getAdmin()) {
            // 建表
//            createSchemaTables(admin);
            // 建表（16位键预分区）
            createTable(admin, getHexSplits("0000000000000000", "ffffffffffffffff", 10));
            // 修改表结构（列族和版本）
//            modifySchema(admin);
            // 查看表结构
            describeTable(admin, "tb1");
            // 列出所有表
            listTable(admin);
            // 删表
//            dropTable(admin, TABLE_NAME);
        }
    }

    private static void dropTable(Admin admin, String tableName) throws IOException {
        TableName table = TableName.valueOf(tableName);
        admin.disableTable(table);
        System.err.println("disable table is success");
        admin.deleteTable(table);
        System.err.println("drop table is success");
    }

    private static void describeTable(Admin admin, String tableName) throws IOException {
        // table descriptors
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        // table columnFamilies descriptors
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        // table columnFamilies name
        Set<byte[]> columnFamilyNames = tableDescriptor.getFamiliesKeys();
        // table columnFamilies number
        System.err.println("columnFamilies number is : " + columnFamilies.length);
        // region replication number
        tableDescriptor.getRegionReplication();
        System.err.println("policyClassName is : " + tableDescriptor.getFlushPolicyClassName());
        /**
         * 遍历 列族名称 和 列族所有的描述信息
         */
        for (byte[] columnFamilyName : columnFamilyNames) {
            System.err.println("columnFamilyName is : " + new String(columnFamilyName));
        }
        for (HColumnDescriptor columnFamily : columnFamilies) {
            System.err.println("HColumnDescriptor is : " + columnFamily);
        }
    }

    private static void listTable(Admin admin) throws IOException {
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.err.println("tableName is : " + tableName);
        }
    }

    private static void modifySchema(Admin admin) throws IOException {

        TableName tableName = TableName.valueOf(TABLE_NAME);
        // 修改表之前，判断表是否存在，防止报异常
        if (!admin.tableExists(tableName)) {
            System.out.println("Table does not exist.");
            System.exit(-1);
        }
        // 创建表对象
        HTableDescriptor table = admin.getTableDescriptor(tableName);
        // 创建列族对象
        HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
        // 设置压缩格式
        newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
        // 执行最大版本号
        newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        // 新增表列族
        admin.addColumn(tableName, newColumn);
        // 修改存在 column family 的压缩格式和版本
        HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
        existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
        existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        table.modifyFamily(existingColumn);
        admin.modifyTable(tableName, table);
        // Disable an existing table
        admin.disableTable(tableName);
        // Delete an existing column family
        admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));
        // Delete a table (Need to be disabled first)
        admin.deleteTable(tableName);
    }

    private static void createSchemaTables(Admin admin) throws IOException {
        // 设置 table name
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        // 设置 列族，压缩
        table.addFamily(new HColumnDescriptor(CF_DEFAULT)
                .setCompressionType(Compression.Algorithm.NONE));
        // 预设 region 数量 和 region 算法
        table.setRegionReplication(2);
        table.setRegionSplitPolicyClassName(RegionSplitter.UniformSplit.class.toString());

        System.out.print("Creating table Start");
        // 如果表存在，先 disable 表，在 delete 表
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        // 实际的创建表函数
        admin.createTable(table);
        System.out.println("Create table Done");
    }

    /**
     * 以下case说明了如何16位键预分区
     *
     * @param admin
     * @param splits
     * @return
     * @throws IOException
     */
    public static boolean createTable(Admin admin, byte[][] splits)
            throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        table.addFamily(new HColumnDescriptor(CF_DEFAULT));
        try {
            admin.createTable(table, splits);
            return true;
        } catch (TableExistsException e) {
            logger.info("table " + table.getNameAsString() + " already exists");
            // the table already exists...
            return false;
        }
    }

    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            System.out.println(String.format("%016x", key));
            splits[i] = b;
        }
        return splits;
    }

}
