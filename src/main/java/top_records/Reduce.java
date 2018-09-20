package top_records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

import static top_records.Main.N_TOP;

public class Reduce extends Reducer<NullWritable, TupleWritable, NullWritable, Text> {

    private PriorityQueue<TupleWritable> pq = new PriorityQueue<>();

    @Override
    protected void reduce(NullWritable key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
        for(TupleWritable t : values){
//            context.write(NullWritable.get(), t.getValue());
            if(pq.size() < N_TOP || t.compareTo(pq.peek())>=0){
                pq.add(new TupleWritable(t.getKey().get(),t.getValue().toString()));

            }
            if(pq.size() > N_TOP)
                pq.poll();
        }
        for(;pq.size()>0;)
            context.write(NullWritable.get(),pq.poll().getValue());
    }
}
