package avg_workhours;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

class TupleWritable implements WritableComparable<TupleWritable> {

    private IntWritable count;
    private DoubleWritable sum;

    public TupleWritable(){
        this(0,0);
    }

    public TupleWritable(int count, double sum){
        this(new IntWritable(count), new DoubleWritable(sum));
    }

    public TupleWritable(IntWritable count, DoubleWritable sum){
        this.count = count;
        this.sum = sum;
    }

    public IntWritable getCount() {
        return count;
    }

    public DoubleWritable getSum() {
        return sum;
    }

    @Override
    public int compareTo(TupleWritable o) {
        if(getCount().compareTo(o.count)!=0)
            return getCount().compareTo(o.count);
        return getSum().compareTo(o.sum);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        getCount().write(dataOutput);
        getSum().write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        getCount().readFields(dataInput);
        getSum().readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleWritable that = (TupleWritable) o;
        return Objects.equals(count, that.count) &&
                Objects.equals(sum, that.sum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, sum);
    }
}
