package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class TotalSort extends Configured implements Tool {


    static class SimpleMapper extends
            Mapper<LongWritable, Text, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            LongWritable longWritable = new LongWritable(Integer.parseInt(value.toString()));
            context.write(longWritable, longWritable);
        }
    }

    static class SimpleReducer extends
            Reducer<LongWritable, LongWritable, LongWritable, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                              Context context) throws IOException, InterruptedException {
            for (LongWritable value : values)
                context.write(value, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("<input> <output>");
            System.exit(127);
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(TotalSort.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        job.setJobName("TotalSort");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4");
        int exitCode = ToolRunner.run(new TotalSort(), args);
        System.exit(exitCode);
    }
}

