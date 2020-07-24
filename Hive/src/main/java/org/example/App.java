package org.example;

import java.sql.*;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, SQLException {

        Connection conn= DriverManager.getConnection("jdbc:hive2://ld1:2181,s1:2181,s2:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");

        Statement stmt= conn.createStatement();

        String querySql= "show databases";


        ResultSet res=stmt.executeQuery(querySql);

        while (res.next())
            System.out.println(res.getString(1));


        res.close();
        stmt.close();
        conn.close();



        System.out.println( "Hello World!" );
    }
}
