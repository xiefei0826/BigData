package org.example.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import java.util.StringTokenizer;

public class WordCount {
    private static Logger logger = Logger.getLogger(WordCount.class);

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text keyOut;
            IntWritable valueOut = new IntWritable(1);
            StringTokenizer token = new StringTokenizer(value.toString());
            while (token.hasMoreTokens()) {
                keyOut = new Text(token.nextToken());
                context.write(keyOut, valueOut);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static void CreateFile() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        FSDataOutputStream stream = hdfs.create(new Path("/mapreduce/file1"), true);
        stream.write("Hello World \n Bye World".getBytes());
        stream.flush();
        stream.close();
        stream = hdfs.create(new Path("/mapreduce/file2"), true);
        stream.write("Hello Hadoop \n Bye Hadoop".getBytes());
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
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            String otherArg = otherArgs[i];
            FileInputFormat.addInputPath(job, new Path(otherArg));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
