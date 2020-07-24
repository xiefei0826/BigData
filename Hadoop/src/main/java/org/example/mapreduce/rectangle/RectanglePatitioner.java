package org.example.mapreduce.rectangle;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RectanglePatitioner extends Partitioner<RectangleWritable, NullWritable> {


    @Override
    public int getPartition(RectangleWritable k2, NullWritable v2, int numReduceTasks) {

        if (k2.getLength() == k2.getWidth())
            return 0;
        return 1;
    }
}
