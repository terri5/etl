/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.etl.test;

import com.bigdata.etl.util.DwUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 * @author GZETL
 */
public class TestETL {
    
    public TestETL() {
    }
    
    @Test
    public void testExport(){
     //   HbaseUtil.Scan("DEVICE_LOG_APP","20161110000000"); 
    // DwUtil.bulkInsert("base.device_log_app_2016",null);
    }
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
