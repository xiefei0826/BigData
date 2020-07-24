package org.example.mapreduce.rectangle;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RectangleWritable implements WritableComparable {
    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    int length, width;


    public RectangleWritable(int length, int width) {
        this.length = length;
        this.width = width;
    }

    public RectangleWritable() {

    }


    @Override
    public int compareTo(Object o) {
        RectangleWritable to = (RectangleWritable) o;
        if (this.getLength() * this.getWidth() > to.getLength() * to.getWidth())
            return 1;
        else if (this.getLength() * this.getWidth() < to.getLength() * to.getWidth())
            return -1;
        return 0;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(length);
        out.writeInt(width);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.length = in.readInt();
        this.width = in.readInt();
    }


}
