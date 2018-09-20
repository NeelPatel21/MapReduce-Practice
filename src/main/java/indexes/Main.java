package indexes;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
    public static void main(String ... arg) throws Exception {
        System.exit(ToolRunner.run(new Main(),arg));
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("building indexes");
        job.setJarByClass(Main.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " |- ");

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        FileInputFormat.setInputDirRecursive(job, true);

        //TODO enable recursive traveling in input directory.

        return job.waitForCompletion(true)?0:1;
    }
}
