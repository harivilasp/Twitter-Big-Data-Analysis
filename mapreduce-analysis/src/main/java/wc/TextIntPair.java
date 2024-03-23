package wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextIntPair implements Writable {
    private Text first;
    private IntWritable second;

    public TextIntPair() {
        set(new Text(), new IntWritable());
    }

    public TextIntPair(Text first, IntWritable second) {
        set(first, second);
    }

    public void set(Text first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
}
