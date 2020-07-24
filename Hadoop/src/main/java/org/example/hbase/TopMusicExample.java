package org.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TopMusicExample {
    private static Logger logger = Logger.getLogger(TopMusicExample.class);

    static class MyMapper extends TableMapper<Text, IntWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            List<Cell> cells = value.listCells();
            // 取出 中的所有 单元 ，实际 上只扫描了一列(info:name) 。
            for (Cell cell : cells) {
                context.write(new Text(Bytes.toString(CellUtil.cloneValue(cell))), new IntWritable(1));
            }
        }
    }

    static class MyReduce extends TableReducer<Text, IntWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            AtomicInteger playCount = new AtomicInteger();
            logger.info("playcount: " + playCount.get());
            values.forEach(value -> {
                playCount.addAndGet(value.get());
            });
            //为put 操作指定 行键
            Put put = new Put(Bytes.toBytes(key.toString()));

            //为Put操作指定 列和值
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("rank"), Bytes.toBytes(playCount.get()));

            context.write(key, put);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        Configuration conf = HbaseUtil.conf;
        conf.set("mapred.jar", "/home/xf/code/JavaTemplate/Hadoop/target/Hadoop-1.0-SNAPSHOT.jar");
        conf.set("fs.defaultFS", "hdfs://ld1:8020");


        Job job = Job.getInstance(conf, "top-music");

        job.setJarByClass(TopMusicExample.class);

        job.setNumReduceTasks(1);

        //为music设置 过虑 条件  .只保留歌名，name
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        //使用hbase 提供 的工具 类来设置 job
        TableMapReduceUtil.initTableMapperJob("music1", scan, MyMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("namelist", MyReduce.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        logger.info("end");
    }
}
