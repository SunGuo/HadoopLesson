package com.rongxin.hadoop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by guoxian1 on 15/4/18.
 */
public class HbaseTest1 {


        private static Configuration conf = null;

        /**
         * 初始化配置
         */
        static {
            Configuration HBASE_CONFIG = new Configuration();
            //与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同
            HBASE_CONFIG.set("hbase.zookeeper.quorum", "master,slave1,slave2");
            //与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同
            HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
            conf = HBaseConfiguration.create(HBASE_CONFIG);
        }

        /**
         * 创建一张表
         */
        public static void creatTable(String tableName, String[] familys) throws Exception {
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (admin.tableExists(tableName)) {
                System.out.println("table already exists!");
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for(int i=0; i<familys.length; i++){
                    tableDesc.addFamily(new HColumnDescriptor(familys[i]));
                }
                admin.createTable(tableDesc);
                System.out.println("create table " + tableName + " ok.");
            }
        }

        /**
         * 删除表
         */
        public static void deleteTable(String tableName) throws Exception {
            try {
                HBaseAdmin admin = new HBaseAdmin(conf);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("delete table " + tableName + " ok.");
            } catch (MasterNotRunningException e) {
                e.printStackTrace();
            } catch (ZooKeeperConnectionException e) {
                e.printStackTrace();
            }
        }

        /**
         * 插入一行记录
         */
        public static void addRecord (String tableName, String rowKey, String family, String qualifier, String value)
                throws Exception{
            try {
                HTable table = new HTable(conf, tableName);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
                table.put(put);
                System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 删除一行记录
         */
        public static void delRecord (String tableName, String rowKey) throws IOException{
            HTable table = new HTable(conf, tableName);
            List list = new ArrayList();
            Delete del = new Delete(rowKey.getBytes());
            Delete del1 = new Delete("temp".getBytes());

            list.add(del);
            list.add(del1);
            table.delete(list);
            System.out.println("del recored " + rowKey + " ok.");
        }

        /**
         * 查找一行记录
         */
        public static void getOneRecord (String tableName, String rowKey) throws IOException{
            HTable table = new HTable(conf, tableName);
            Get get = new Get(rowKey.getBytes());
            Result rs = table.get(get);
            for(KeyValue kv : rs.raw()){
                System.out.print(new String(kv.getRow()) + " " );
                System.out.print(new String(kv.getFamily()) + ":" );
                System.out.print(new String(kv.getQualifier()) + " " );
                System.out.print(kv.getTimestamp() + " " );
                System.out.println(new String(kv.getValue()));
            }
        }

        /**
         * 显示所有数据
         */
        public static void getAllRecord (String tableName) {
            try{
                HTable table = new HTable(conf, tableName);
                Scan s = new Scan();
                ResultScanner ss = table.getScanner(s);
                for(Result r:ss){
                    for(KeyValue kv : r.raw()){
                        System.out.print(new String(kv.getRow()) + " ");
                        System.out.print(new String(kv.getFamily()) + ":");
                        System.out.print(new String(kv.getQualifier()) + " ");
                        System.out.print(kv.getTimestamp() + " ");
                        System.out.println(new String(kv.getValue()));
                    }
                }
            } catch (IOException e){
                e.printStackTrace();
            }
        }

        public static void  main (String [] agrs) {
            try {

                String tablename = "scores4";
                String[] familys = {"grade", "course"};
              //  HbaseTest1.creatTable(tablename, familys);

                //add record zkb
                /**
                 *  put scores4 lvxiaoyang grade 5
                 *  put scores4 lvxiaoyang course:math 17
                 *  put scores4 lvxiaoyang course:art 101
                 *  put scores4 lvxiaoyang course:english 99
                  */

//               HbaseTest1.addRecord(tablename,"lvxiaoyang","grade","","5");
//               HbaseTest1.addRecord(tablename,"lvxiaoyang","course","english","90");
//               HbaseTest1.addRecord(tablename,"lvxiaoyang","course","math","97");
//               HbaseTest1.addRecord(tablename,"lvxiaoyang","course","art","87");
//               HbaseTest1.addRecord(tablename,"jiangwei","grade","","5");
//               HbaseTest1.addRecord(tablename,"jiangwei","course","english","90");
//               HbaseTest1.addRecord(tablename,"jiangwei","course","math","97");
//               HbaseTest1.addRecord(tablename,"jiangwei","course","art","87");

                //add record  baoniu
            //    HbaseTest1.addRecord(tablename,"haotian","grade","","4");
             //   HbaseTest1.addRecord(tablename,"haotian","course","math","89");

              //  System.out.println("===========get one record========");
              //  HbaseTest1.getOneRecord(tablename, "lvxiaoyang");
              //  System.out.println("===========show all record========");
            //    HbaseTest1.getAllRecord(tablename);

//               System.out.println("===========del one record========");
//               HbaseTest1.delRecord(tablename, "lvxiaoyang");
//               HbaseTest1.getAllRecord(tablename);

                HbaseTest1.deleteTable("scores4");

              //  System.out.println("===========show all record========");
              //  HbaseTest1.getAllRecord(tablename);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
}
