package org.example.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.example.Tools;

import javax.print.attribute.standard.MediaSize;
import java.io.IOException;

public class Sort {
    private static Logger logger = Logger.getLogger(Sort.class);

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable lineNum = new IntWritable(1);

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(lineNum, key);
                lineNum = new IntWritable(lineNum.get() + 1);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path input1 = new Path("/mapreduce/sort/file1");
        Path input2 = new Path("/mapreduce/sort/file2");
        Path input3 = new Path("/mapreduce/sort/file3");

        Tools.CreateFile(input1, "2\n" +
                "32\n" +
                "654\n" +
                "32\n" +
                "15\n" +
                "756\n" +
                "65223\n");
        Tools.CreateFile(input2, "5956\n" +
                "22\n" +
                "650\n" +
                "92\n");
        Tools.CreateFile(input3, "26\n" +
                "54\n" +
                "6\n");

        Configuration conf = new Configuration();
        conf.set("mapreduce.jobtracker.address", "ld1:9001");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            logger.error("args error ");
            System.exit(2);
        }
        Job job = new Job(conf, "Data Sort");
        job.setJarByClass(Sort.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
