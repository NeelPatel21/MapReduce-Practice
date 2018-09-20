package top_records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TupleWritable implements WritableComparable<TupleWritable>{

    private IntWritable key;
    private Text value;

    public TupleWritable(){
        this(0,"");
    }

    public TupleWritable(IntWritable key, Text value){
        this.key = key;
        this.value = value;
    }

    public TupleWritable(int key, String value){
        this(new IntWritable(key), new Text(value));
    }

    public TupleWritable(int key, Object value){
        this(key, value.toString());
    }

    public IntWritable getKey() {
        return key;
    }

    public Text getValue() {
        return value;
    }

    public void setKey(IntWritable key) {
        this.key = key;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    @Override
    public int compareTo(TupleWritable o) {
        if(getKey().compareTo(o.getKey()) != 0)
            return getKey().compareTo(o.getKey());
        return getValue().compareTo(o.getValue());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        value.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleWritable that = (TupleWritable) o;
        return Objects.equals(getKey(), that.getKey()) &&
                Objects.equals(getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKey(), getValue());
    }
}
