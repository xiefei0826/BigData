package org.example.mapreduce.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(",");
        Long id = Long.parseLong(arr[0]);

        if (id < 5) {
            Text sw = new Text();
            String[] write = new String[]{arr[3], arr[7]};
            sw.set(StringUtils.join(',', write));

            LongWritable lw = new LongWritable();
            lw.set(id);
            context.write(lw, sw);
            context.write(lw, sw);
        }

//        super.map(key, value, context);
    }
}
