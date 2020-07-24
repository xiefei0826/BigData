package org.example.mapreduce.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceExample {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("MapReduce");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("testJob");
        job.setJarByClass(MapReduceExample.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, "/hadoop/test/memorylib/part-m-00000");
        FileOutputFormat.setOutputPath(job, new Path("/output/test8"));
        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(exitCode);
    }
}
