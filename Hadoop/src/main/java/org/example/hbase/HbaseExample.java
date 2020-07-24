package org.example.hbase;

import org.apache.log4j.Logger;

public class HbaseExample {
    private static Logger logger = Logger.getLogger(HbaseExample.class);

    public static void main(String[] args) {

        logger.info("start");
//        HbaseUtil.CreateTable("test", "info", "asset");
        HbaseUtil.Insert("test", "1000", "info", "name", "xf");
        logger.info("end");
    }
}
