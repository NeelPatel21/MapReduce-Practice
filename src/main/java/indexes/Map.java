package indexes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //get file-name from context
        Text fileName = new Text(((FileSplit) context.getInputSplit()).getPath().getName());

        //iterate through string and write pair of word & file-name to context.
        for(String token: value.toString().split(" "))
            context.write(new Text(token.trim()), fileName);
    }
}
