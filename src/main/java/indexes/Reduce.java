package indexes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // combine values into string & write it to context
        StringBuilder sb = new StringBuilder();
        values.forEach(x->sb.append(x.toString().trim()+" | "));
        String value = sb.substring(0,sb.length()-3);
        context.write(key, new Text(value));
    }
}
