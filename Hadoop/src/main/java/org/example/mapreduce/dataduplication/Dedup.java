package org.example.mapreduce.dataduplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.example.Tools;

import java.io.IOException;

public class Dedup {
    private static Logger logger = Logger.getLogger(Dedup.class);

    //map 将输入中的key复制到输出数据的key上并直接输出
    public static class Map extends Mapper<Object, Text, Text, Text> {

        private static Text line = new Text();// 每行数据

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            context.write(line, new Text(""));
        }
    }

    //Reduce将输入中的key复制 到输出 数据 的key上并直接 输出
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path file1 = new Path("/mapreduce/dedup/1");
        Path file2 = new Path("/mapreduce/dedup/2");

        Tools.CreateFile(file1, "2020-3-1 a\n" + "\n" + "2020-3-2 b\n" + "\n" + "2020-3-3 c\n" + "\n" + "2020-3-4 d\n" + "\n" + "2020-3-5 a\n" + "\n" + "2020-3-6  b\n" + "\n" + "2020-3-7 c\n" + "\n" + "2020-3-3 c\n");
        Tools.CreateFile(file2, "2020-3-1 b\n" + "2020-3-2 a\n" + "2020-3-3 b\n" + "2020-3-4 d\n" + "2020-3-5 a\n" + "2020-3-6  c\n" + "2020-3-7 d\n" + "2020-3-3 c");

        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "ld1:9001");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            logger.error("param error");
            System.exit(2);
        }

        Job job = new Job(conf, "Dedup");
        job.setJarByClass(Dedup.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        ;

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
