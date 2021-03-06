/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.etl.util;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author GZETL
 */
public class HbaseUtil {

    static final Configuration cfg = HBaseConfiguration.create();
    static Connection connection;

    static {
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.zookeeper.quorum", "172.16.2.41,172.16.2.42,172.16.2.43");
        cfg.setInt("hbase.rpc.timeout", 20000);
        cfg.setInt("hbase.client.retries.number", 1000*6);
        //cfg.setInt("hbase.client.scanner.caching", 100);
        cfg.setInt("hbase.regionserver.lease.period", 6000*5);
        cfg.setInt("zookeeper.recovery.retry", 3);
        cfg.setInt("zookeeper.recovery.retry.intervalmill", 200);
        cfg.setInt("hbase.client.operation.timeout", 30000*5);

        // cfg.set("hbase.master", "airmediahbasev3.azurehdinsight.cn:60000");
        try {
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException ex) {
            Logger.getLogger(HbaseUtil.class.getName()).log(Level.SEVERE, "初始化configuration异常", ex);
        }

    }

    public static ResultScanner Scan(String tableName, String rowkeyPrefix) {

        //  try (Connection conn = ConnectionFactory.createConnection(cfg)) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {

            Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator(Bytes.toBytes(rowkeyPrefix)));
            // EQUAL 

            Scan s = new Scan();
            s.setMaxResultSize(10000);
            s.setCaching(1000);
            s.setFilter(filter1);
            ResultScanner rs = table.getScanner(s);

            return rs;

        } catch (IOException ex) {
            Logger.getLogger(HbaseUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        /*
            } catch (IOException ex) {
                Logger.getLogger(HbaseUtil.class.getName()).log(Level.SEVERE, null, ex);
            }
         */
        return null;
    }

    public static ResultScanner Scan(String tableName, String startRow, String endRow) {

        //  try (Connection conn = ConnectionFactory.createConnection(cfg)) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {

            Scan s = new Scan();
            s.setMaxResultSize(10000);
            s.setCaching(1000);
            s.setStartRow(Bytes.toBytes(startRow));
            s.setStopRow(Bytes.toBytes(endRow));
            ResultScanner rs = table.getScanner(s);
            return rs;

        } catch (IOException ex) {
            Logger.getLogger(HbaseUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        /*
            } catch (IOException ex) {
                Logger.getLogger(HbaseUtil.class.getName()).log(Level.SEVERE, null, ex);
            }
         */
        return null;
    }
}
