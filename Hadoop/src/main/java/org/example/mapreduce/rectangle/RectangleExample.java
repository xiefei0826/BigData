package org.example.mapreduce.rectangle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class RectangleExample {
    private static Logger logger = Logger.getLogger(RectangleExample.class);

    static class RectangleMapper extends Mapper<LongWritable, Text, RectangleWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splites = value.toString().split(" ");
            RectangleWritable outKey = new RectangleWritable(Integer.parseInt(splites[0]), Integer.parseInt(splites[1]));
            context.write(outKey, NullWritable.get());
        }
    }

    static class RectangleReduce extends Reducer<RectangleWritable, NullWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(RectangleWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable length = new IntWritable(key.getLength());
            IntWritable width = new IntWritable(key.getWidth());
            logger.info("length : " + length + " width : " + width);
            context.write(length, width);
        }
    }

    private static void CreateFile() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        FSDataOutputStream stream = hdfs.create(new Path("/mapreduce/rectangle"), true);
        stream.write("9 9\n4 5\n7 8\n1 1\n3 6".getBytes());
        stream.flush();
        stream.close();
        hdfs.close();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        CreateFile();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            logger.error("Usage: wordcount ");
            System.exit(2);
        }
        Job job = new Job(conf, "rectangle");
        job.setJarByClass(RectangleExample.class);

        job.setMapperClass(RectangleMapper.class);

//        job.setCombinerClass(RectangleReduce.class);
        job.setReducerClass(RectangleReduce.class);

        job.setMapOutputKeyClass(RectangleWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(RectanglePatitioner.class);
        job.setNumReduceTasks(2);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            String otherArg = otherArgs[i];
            FileInputFormat.addInputPath(job, new Path(otherArg));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
