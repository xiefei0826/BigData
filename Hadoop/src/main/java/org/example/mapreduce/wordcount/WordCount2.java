package org.example.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.example.Tools;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount2 {
    private static Logger logger = Logger.getLogger(WordCount2.class);

    private static class IntWritableDecreaseingComparator extends IntWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // 比较结果取负数，则是降序排序。
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
        FSDataOutputStream stream = hdfs.create(new Path("/mapreduce/wordcount2/file1"), true);
        stream.write("Hello World \n Bye World".getBytes());
        stream.flush();
        stream.close();
        stream = hdfs.create(new Path("/mapreduce/wordcount2/ile2"), true);
        stream.write("Hello Hadoop \n Bye Hadoop".getBytes());
        stream.flush();
        stream.close();
        hdfs.close();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        CreateFile();
        Configuration conf = new Configuration();
        Path tmpDir = new Path("hdfs://ld1:9000/mapreduce/wordcount2/tmp");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            logger.error("Usage: wordcount ");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount2.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, tmpDir);

        if (job.waitForCompletion(true)) {
            Job sortJob = new Job(conf, "sort");
            sortJob.setJarByClass(WordCount2.class);

            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(sortJob, tmpDir);
            // hadoop 提供 ，将key value 切换
            sortJob.setMapperClass(InverseMapper.class);
            sortJob.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));

            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);

            sortJob.setOutputFormatClass(TextOutputFormat.class);
            sortJob.setSortComparatorClass(IntWritableDecreaseingComparator.class);
            if (sortJob.waitForCompletion(true))
                logger.info("ok");
            else
                logger.error("error");
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
