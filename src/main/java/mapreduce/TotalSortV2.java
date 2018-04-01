package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class TotalSortV2 extends Configured implements Tool {
    static class SimpleMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            LongWritable intWritable = new LongWritable(Long.parseLong(value.toString()));
            context.write(intWritable, intWritable);
        }
    }

    static class SimpleReducer extends Reducer<LongWritable, LongWritable, LongWritable, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                              Context context) throws IOException, InterruptedException {
            for (LongWritable value : values)
                context.write(value, NullWritable.get());
        }
    }

    public static class RangPartitioner extends Partitioner<LongWritable, LongWritable> {
        @Override
        public int getPartition(LongWritable key, LongWritable value, int numPartitions) {
            long keyInt = Long.parseLong(key.toString());
            if (keyInt < 10000) {
                return 0;
            } else if (keyInt < 20000) {
                return 1;
            } else {
                return 2;
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("<input> <output>");
            System.exit(127);
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(TotalSortV2.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);
        job.setPartitionerClass(RangPartitioner.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(3);
        job.setJobName("dw_subject");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4");
        int exitCode = ToolRunner.run(new TotalSortV2(), args);
        System.exit(exitCode);
    }
}

