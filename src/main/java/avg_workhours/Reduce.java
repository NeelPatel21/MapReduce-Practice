package avg_workhours;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, TupleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
        int c=0;
        double s=0;
        for(TupleWritable value:values){
            s+=value.getSum().get();
            c+=value.getCount().get();
        }
        context.write(key, new DoubleWritable(s/c));
    }
}
