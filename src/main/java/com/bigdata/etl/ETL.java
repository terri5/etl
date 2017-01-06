/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.etl;

import com.bigdata.etl.util.DwUtil;
import com.bigdata.etl.util.HbaseUtil;
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
    private static  String DW_TTABLE="BASE.DEVICE_LOG_APP_2016";
    private static  String HBASE_TABLE="DEVICE_LOG_APP";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("至少两个参数到小时或者天");
            System.exit(-1);
        }
         if (!"app".equals(args[0])&&!"hit".equals(args[0])) {
            System.err.println("非法的数据项 " + args[0]);
            System.exit(-1);
        }
        if (args[1].length() != 10 && args[1].length() != 8) {
            System.err.println("非法的参数到小时或者天 " + args[0]);
            System.exit(-1);
        }
        if(args[0].toLowerCase().equals("hit")){
            DW_TTABLE="BASE.DEVICE_LOG_HIT_BASE_2016";
            HBASE_TABLE="DEVICE_LOG_HIT";
        }
        String time = args[1];
        List<String> keys = new ArrayList<>();
        if (args[1].length() == 10) {
            for (int i = 0; i < 60; i++) {     
                    keys.add(time.substring(0, 8) + String.format("%02d", i) + time.substring(8));
            }
        } else {
            for (int i = 0; i < 60; i++) {
              keys.add(time.substring(0, 8) + String.format("%02d", i));
            }

        }
        System.out.println(LocalTime.now() + " 参数是：" + args[0]+" "+ args[1]);
        keys.parallelStream().forEach(key -> {
          //  System.out.println(LocalTime.now() +" "+Thread.currentThread().getId()+" key=" + key);
      //      ResultScanner rs = HbaseUtil.Scan("DEVICE_LOG_APP", key);
           String start=key;
           String end="";
           if(start.length()==10)
            end=start.substring(0,8)+String.format("%02d",Integer.parseInt(start.substring(8,10))+1);
           else
            end=start.substring(0,10)+String.format("%02d",Integer.parseInt(start.substring(10,12))+1);
         //  System.out.println(LocalTime.now() +" "+Thread.currentThread().getId()+" "+start+" "+end);
           ResultScanner rs=HbaseUtil.Scan(HBASE_TABLE,start,end);
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
               
                if (lst.size() == 500000) {
                    DwUtil.bulkInsert(DW_TTABLE, lst);
                    lst = new ArrayList();
                }
            }
            rs.close();
            if (lst.size() > 0) {
                    DwUtil.bulkInsert(DW_TTABLE, lst);
            };
         //   System.out.println(LocalTime.now() +" "+Thread.currentThread().getId()+" finished key=" + key);
        });
        System.out.println(LocalTime.now() + " 全部完成 " + args[0]+" "+args[1]);
    }
}
