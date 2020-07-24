package org.example.mapreduce.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        if (key.get() < 10) {
            Text sw = new Text();
            int count = 1;
            for (Text ignored : values) count++;
            sw.set(String.valueOf(count));
            context.write(key, sw);

        }

    }
}
