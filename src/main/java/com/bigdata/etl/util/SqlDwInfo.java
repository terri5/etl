/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.etl.util;

/**
 *
 * @author GZETL
 */
public interface SqlDwInfo {

    static String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";//加载驱动程序 
    static String url = "";
    static String user = "amsqldwsa";
    static String password = "";
}
