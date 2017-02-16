/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.etl;

import com.bigdata.etl.util.Consumer;
import com.bigdata.etl.util.DwUtil;
import com.bigdata.etl.util.HbaseUtil;
import java.io.UnsupportedEncodingException;
import java.time.LocalTime;
import java.util.ArrayList;

import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

    public static String DW_TTABLE = "BASE.DEVICE_LOG_APP_2017";
    private static String HBASE_TABLE = "DEVICE_LOG_APP_2017";
    private static BlockingQueue queue = new ArrayBlockingQueue(1000000);
    public static boolean processing = true;

    public static void main(String[] args) {
        int span = 1;
        if (args.length < 2 ) {
            System.err.println("至少两个参数到小时或者天 " + args.length);
            System.exit(-1);
        }
      if (args.length > 3) {
            System.err.println("最多3个参数到小时或者天 " + args[3]);
            System.exit(-1);
        }
        if (!"app".equals(args[0]) && !"hit".equals(args[0])) {
            System.err.println("非法的数据项 " + args[0]);
            System.exit(-1);
        }
        if (args.length == 3 && (args[1].length() != 10 || !args[2].matches("\\d{1,2}") || Integer.parseInt(args[2]) > 23)) {
            System.err.println("非法的时间跨度参数 " + args[2]);
            System.exit(-1);

        }
        if (args.length == 3) {
            span = Integer.parseInt(args[2]);
        }

        if (args[1].length() != 10 && args[1].length() != 8) {
            System.err.println("非法的参数到小时或者天 " + args[0]);
            System.exit(-1);
        }
        if (args[0].toLowerCase().equals("hit")) {
            DW_TTABLE = "BASE.DEVICE_LOG_HIT_BASE_2016";
            HBASE_TABLE = "DEVICE_LOG_HIT_2017";
        }

        String time = args[1];
        List<String> keys = new ArrayList<>();
        if (args[1].length() == 10) {//到小时
            for (int i = 0; i < 60; i++) {
                keys.add(time.substring(0, 8) + String.format("%02d", i) + time.substring(8));
            }
            if (args.length > 2) {
                GregorianCalendar gc = new GregorianCalendar();
                gc.set(GregorianCalendar.YEAR, Integer.parseInt(time.substring(0, 4)));
                gc.set(GregorianCalendar.MONTH, Integer.parseInt(time.substring(4, 6)));
                gc.set(GregorianCalendar.DAY_OF_MONTH, Integer.parseInt(time.substring(6, 8)));
                gc.set(GregorianCalendar.HOUR_OF_DAY, Integer.parseInt(time.substring(8, 10)));
                int day1 = gc.get(GregorianCalendar.DAY_OF_MONTH);
                gc.add(GregorianCalendar.HOUR_OF_DAY, span);
                if (day1 != gc.get(GregorianCalendar.DAY_OF_MONTH)) {
                    System.err.println("不允许跨天抽取 " + args[1] + " " + args[2]);
                    System.exit(-1);

                }
            }

        } else {//到天
            for (int i = 0; i < 60; i++) {
                keys.add(time.substring(0, 8) + String.format("%02d", i));
            }

        }
        final int s2 = span;
        System.out.println(LocalTime.now() + " 参数是：" + args[0] + " " + args[1] + " " + s2);
        /*
        Consumer consumer = new Consumer(queue);
        Thread ct = new Thread(consumer);
        ct.start();
        */
        keys.parallelStream().forEach((String key) -> {
            //System.out.println(LocalTime.now() + " " + Thread.currentThread().getId() + " key=" + key);
            //      ResultScanner rs = HbaseUtil.Scan("DEVICE_LOG_APP", key);
            String start = key;
            String end = "";
            if (start.length() == 10) {//到日期             
                end = start.substring(0, 8) + String.format("%02d", Integer.parseInt(start.substring(8, 10)) + 1);
            } else //12位到小时            
            {
                end = start.substring(0, 10) + String.format("%02d", Integer.parseInt(start.substring(10, 12)) + s2);
            }
            // System.out.println(LocalTime.now() +" "+Thread.currentThread().getId()+" "+start+" "+end);
            ResultScanner rs = HbaseUtil.Scan(HBASE_TABLE, start, end);
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
                /**
                if (lst.size() == 10000) {
                    queue.addAll(lst);
                    lst = new ArrayList();
                }
                */
                 if (lst.size() == 200000) { 
                     DwUtil.bulkInsert(DW_TTABLE,lst);
                     lst = new ArrayList(); 
                     System.gc(); 
                 }

            }
            rs.close();
            if (lst.size() > 0) {
                  DwUtil.bulkInsert(DW_TTABLE, lst);
               // queue.addAll(lst);
            };
               System.gc();
             System.out.println(LocalTime.now() + " " + Thread.currentThread().getId() + " finished key=" + key + " end key=" + end);
        });
        processing = false;
        /*
        try {
            ct.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(ETL.class.getName()).log(Level.SEVERE, null, ex);
        }
*/
        System.out.println(LocalTime.now() + " 全部完成 " + args[0] + " " + args[1] + " " + span);
    }
}
