package avg_workhours;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.nio.file.Files;

public class Main extends Configured implements Tool{
    public static void main(String ... args) throws Exception {
        int status =ToolRunner.run(new Main(),args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combiner.class);

        job.setMapOutputValueClass(TupleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        System.out.println("Input Path :- "+input);
        System.out.println("Output Path :- "+output);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(Main.class);

//        System.out.println("check 2");
        return job.waitForCompletion(true)? 0: 1;
    }
}
