package avg_workhours;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class Map extends Mapper<LongWritable, Text, Text, TupleWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String ar[] = Arrays.stream(value.toString().split(","))
//                            .map(String::trim) // remove space
//                            .toArray(String[]::new);
        String ar[] = value.toString().split(", ");

        String mStat = ar[5]; // Marital status
        double wHours = Double.parseDouble(ar[12]); // working hours

//        System.out.println("check 1");
        context.write(new Text(mStat),new TupleWritable(1,wHours));
    }
}
