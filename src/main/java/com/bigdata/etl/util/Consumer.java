/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.etl.util;

import com.bigdata.etl.ETL;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author GZETL
 */
public class Consumer implements Runnable{  
  
    protected BlockingQueue queue = null;  
  
    public Consumer(BlockingQueue queue) {  
        this.queue = queue;  
    }  
  
    public void run() {  
         List<Map<String,String>> lst=new ArrayList();
         while(ETL.processing || !queue.isEmpty()){
             if(!queue.isEmpty()){
                 try {
                     lst.add((Map<String, String>) queue.take());
                 } catch (InterruptedException ex) {
                     Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
                 }       
             }else{
                System.out.println(LocalTime.now() + " " + Thread.currentThread().getId() + "没有数据");
                 try {
                     Thread.sleep(1000);
                 } catch (InterruptedException ex) {
                     Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
                 }
             }
           if(lst.size()==500000){
              DwUtil.bulkInsert(ETL.DW_TTABLE, lst);
              System.out.println(LocalTime.now() + " " + Thread.currentThread().getId() + "写入50万");
              lst=new ArrayList();
           }
         
         }
         if(lst.size()>0){
             DwUtil.bulkInsert(ETL.DW_TTABLE, lst);
         }
    }  
}  