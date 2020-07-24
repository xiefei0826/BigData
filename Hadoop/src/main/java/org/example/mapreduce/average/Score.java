package org.example.mapreduce.average;

import org.apache.hadoop.conf.Configuration;
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

public class Score {
    private static Logger logger = Logger.getLogger(Score.class);

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, "\n");
            while (tokenizer.hasMoreElements()) {
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizer.nextToken());
                String name = tokenizerLine.nextToken();
                String score = tokenizerLine.nextToken();
                Text tName = new Text(name);
                IntWritable iScore = new IntWritable(Integer.parseInt(score));
                context.write(tName, iScore);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }
            int average = sum / count;
            context.write(key, new IntWritable(average));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path input1 = new Path("/mapreduce/average/file1");
        Path input2 = new Path("/mapreduce/average/file2");
        Path input3 = new Path("/mapreduce/average/file3");

        Tools.CreateFile(input1, "张三 80\n" +
                "李四 82\n" +
                "王五 84\n" +
                "赵六 86\n");
        Tools.CreateFile(input2, "张三 78\n" +
                "李四 89\n" +
                "王五 96\n" +
                "赵六 67\n");
        Tools.CreateFile(input3, "张三 88\n" +
                "李四 99\n" +
                "王五 66\n" +
                "赵六 77\n");

        Configuration conf = new Configuration();
        conf.set("mapreduce.jobtracker.address", "ld1:9001");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            logger.error("args error ");
            System.exit(2);
        }
        Job job = new Job(conf, "Data score");
        job.setJarByClass(Score.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
