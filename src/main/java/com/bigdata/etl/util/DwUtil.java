/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.etl.util;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.sun.rowset.CachedRowSetImpl;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author GZETL
 */
public class DwUtil {

    static String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";//加载驱动程序 
    static String url = "";
    static String user = "";
    static String password = "";

    public static void bulkInsert(String tableName, List<Map<String, String>> lst) {

        ResultSet rs = null;
        java.sql.Statement stmt = null;

        try (java.sql.Connection conn = DataSource.getConnection()) {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select top 0 * from " + tableName);
            try (SQLServerBulkCopy bulk = new SQLServerBulkCopy(url + "user=" + user + ";password=" + password)) {

                bulk.setDestinationTableName(tableName);
                ResultSetMetaData rsmd = rs.getMetaData();
                if (lst == null) {
                    return;
                }
                // System.out.println(LocalTime.now() + " "+Thread.currentThread().getId()+" 收到写入"+lst.size());
                try (CachedRowSetImpl x = new CachedRowSetImpl()) {
                    x.populate(rs);
                    for (int k = 0; k < lst.size(); k++) {
                        Map<String, String> map = lst.get(k);
                        x.last();
                        x.moveToInsertRow();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            String name = rsmd.getColumnName(i).toUpperCase();
                            int type = rsmd.getColumnType(i);//返回列类型对应的整形数表示。package java.sql.Type类有各类型对应的整数表示。

                            try {
                                switch (type) {
                                    case Types.BIGINT:
                                        if (map.containsKey(name) && map.get(name).matches("\\d{1,}")) {
                                            x.updateLong(i, Long.valueOf(map.get(name)));
                                        } else {
                                            x.updateLong(i, 0);
                                        }
                                        break;
                                    case Types.FLOAT:
                                        if (map.containsKey(name) && map.get(name).matches("([+-]?)\\d*\\.\\d+$")) {
                                            x.updateFloat(i, Float.valueOf(map.get(name)));
                                        } else {
                                            x.updateFloat(i, 0);
                                        }
                                        break;
                                    case Types.DOUBLE:
                                        if (map.containsKey(name) && map.get(name).trim().length() > 0 && StringUtils.isNumeric(map.get(name))) {
                                            x.updateDouble(i, Double.valueOf(map.get(name)));
                                        } else {
                                            x.updateDouble(i, 0);
                                        }
                                        break;

                                    case Types.INTEGER:
                                        if (map.containsKey(name) && map.get(name).matches("\\d{1,}")) {
                                            x.updateInt(i, Integer.valueOf(map.get(name)));
                                        } else {
                                            x.updateInt(i, 0);
                                        }
                                        break;
                                    case Types.VARCHAR:
                                    case Types.NVARCHAR:
                                        int len = rsmd.getColumnDisplaySize(i);
                                        String v = map.get(name);
                                        if (map.containsKey(name)) {
                                            x.updateString(i, v.length() > len ? v.substring(0, len) : v);
                                        } else {
                                            x.updateString(i, "");
                                        }
                                        break;
                                    default:
                                        throw new RuntimeException("未知的数据类型 " + type);
                                }
                                /*
                            if(map.containsKey("SYS_TELECOM"))
                                System.err.println(map.get("SYS_TELECOM"));
                                 */
                            } catch (RuntimeException | SQLException e) {
                                Logger.getLogger(DwUtil.class.getName()).log(Level.SEVERE, "对应的数据类型 name=" + name + " v=" + map.get(name), e);
                            }

                        }
                        x.insertRow();
                        x.moveToCurrentRow();
                        //x.acceptChanges();
                    }

                    long start = System.currentTimeMillis();
                    bulk.writeToServer(x);
                    long end = System.currentTimeMillis();
                    System.out.println(LocalTime.now() + " " + Thread.currentThread().getId() + " 耗时" + (end - start) + "ms" + " 写入" + x.size());
                }
            }

        } catch (SQLException e) {
            Logger.getLogger(DwUtil.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException ex) {
                Logger.getLogger(DwUtil.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

}
