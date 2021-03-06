/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.etl.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import static java.util.concurrent.TimeUnit.*;

/**
 *
 * @author GZETL
 */
public class DataSource implements SqlDwInfo{

    private static final long CONNECTION_TIMEOUT =HOURS.toMillis(1);  

    private static HikariDataSource ds;

    static {
        DataSource ds = new DataSource();
        ds.init(60, 100);
    }

    /**
     * 初始化连接池 
     *
     * @param minimum
     * @param Maximum
     */
    public void init(int minimum, int Maximum) {
        String jurl = url + "user=" + user + ";password=" + password;
        //连接池配置
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverName);
        config.setJdbcUrl(jurl);
        config.addDataSourceProperty("cachePrepStmts", true);
        config.addDataSourceProperty("prepStmtCacheSize", 500);
        config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
        config.setConnectionTimeout(CONNECTION_TIMEOUT);
        //  config.setConnectionTestQuery("SELECT 1");
        config.setAutoCommit(true);
        //池中最小空闲链接数量
        config.setMinimumIdle(minimum);
        //池中最大链接数量
        config.setMaximumPoolSize(Maximum);

        ds = new HikariDataSource(config);

    }

    /**
     * 销毁连接池
     */
    public void shutdown() {
        ds.shutdown();
    }

    /**
     * 从连接池中获取链接
     *
     * @return
     */
    public static Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            ds.resumePool();
            return null;
        }
    }
    /*
   public static void main(String[] args) throws SQLException {
        DataSource ds = new DataSource();
        ds.init(10, 50);
        Connection conn = ds.getConnection();
        //......
        //最后关闭链接
        conn.close();
    }
     */

}
