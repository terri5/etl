/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ihangmei.bigdata.etl;

import com.ihangmei.bigdata.etl.util.DwUtil;
import com.ihangmei.bigdata.etl.util.HbaseUtil;
import java.io.UnsupportedEncodingException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 *
 * @author GZETL
 */
public class ETL {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("至少一个参数到小时或者天");
            System.exit(-1);
        }
        if (args[0].length() != 10 && args[0].length() != 8) {
            System.err.println("非法的参数到小时或者天 " + args[0]);
            System.exit(-1);
        }
        String time = args[0];
        List<String> keys = new ArrayList<>();
        if (args[0].length() == 10) {
            for (int i = 0; i < 60; i++) {
                if (i < 10) {
                    keys.add(time.substring(0, 8) + "0" + i + time.substring(8));
                } else {
                    keys.add(time.substring(0, 8) + i + time.substring(8));
                }
            }
        } else {
            for (int i = 0; i < 60; i++) {
                if (i < 10) {
                    keys.add(time.substring(0, 8) + "0" + i);
                } else {
                    keys.add(time.substring(0, 8) + i);
                }
            }

        }
        System.out.println(LocalTime.now() + " 参数是：" + args[0]);
        keys.parallelStream().forEach(key -> {
          //  System.out.println(LocalTime.now() +" "+Thread.currentThread().getId()+" key=" + key);
            ResultScanner rs = HbaseUtil.Scan("DEVICE_LOG_APP", key);
            List<Map<String, String>> lst = new ArrayList();
            for (Result r : rs) {
                Map<String, String> row = new HashMap<>();
                for (KeyValue keyValue : r.raw()) {
                    try {
                        row.put(new String(keyValue.getQualifier()), new String(keyValue.getValue(), "UTF-8"));
                    } catch (UnsupportedEncodingException ex) {
                        Logger.getLogger(ETL.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                lst.add(row);
                if (lst.size() > 500000) {
                    DwUtil.bulkInsert("base.device_log_app_2016", lst);
                    lst = new ArrayList();
                }
            }
            if (lst.size() > 0) {
                    DwUtil.bulkInsert("base.device_log_app_2016", lst);
            };
         //   System.out.println(LocalTime.now() +" "+Thread.currentThread().getId()+" finished key=" + key);
        });
        System.out.println(LocalTime.now() + " 全部完成 " + args[0]);
    }
}
