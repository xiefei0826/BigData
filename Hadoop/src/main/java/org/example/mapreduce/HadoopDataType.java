package org.example.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.example.hdfs.FileExample;

public class HadoopDataType {

    private static Logger logger = Logger.getLogger(HadoopDataType.class);

    public static void main(String[] args) {
        TestText();
        TestArrayWritable();
        TestMapWritable();
    }

    private static void TestText() {
        logger.info("test Text");
        Text text = new Text("hello hadoop");
        logger.info("length : " + text.getLength());
        logger.info("find a : " + text.find("a"));
        logger.info("to string : " + text.toString());
    }

    private static void TestArrayWritable() {

        logger.info("test ArrayWritable");
        ArrayWritable arr = new ArrayWritable(IntWritable.class);
        IntWritable year = new IntWritable(2020);
        IntWritable month = new IntWritable(4);
        IntWritable day = new IntWritable(22);
        arr.set(new Writable[]{year, month, day});
        logger.info("year : " + ((IntWritable) arr.get()[0]).get());

    }

    private static void TestMapWritable() {
        logger.info("test MapWritable");
        MapWritable map = new MapWritable();
        Text k1 = new Text("name1");
        DoubleWritable v1 = new DoubleWritable(1.111);
        LongWritable k2 = new LongWritable(2);
        NullWritable v2 = NullWritable.get();
        map.put(k1, v1);
        map.put(k2, v2);
        for (Writable writable : map.keySet()) {
            logger.info(map.get(writable));
        }
    }
}
