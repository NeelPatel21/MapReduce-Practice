package top_records;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;

import static top_records.Main.N_TOP;

public class Map extends Mapper <LongWritable, Text, NullWritable, TupleWritable> {

    private PriorityQueue<TupleWritable> pq = new PriorityQueue<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String ar[] = value.toString().split(", ");
        int age = Integer.parseInt(ar[2]);

        TupleWritable t = new TupleWritable(age, value);
        if(pq.size() < N_TOP || t.compareTo(pq.peek())>0){
            pq.add(t);

            if(pq.size() > N_TOP)
                pq.poll();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(;pq.size()>0;)
            context.write(NullWritable.get(), pq.poll());
    }
}
