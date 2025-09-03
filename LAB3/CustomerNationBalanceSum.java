import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomerNationBalanceSum {

    public static class CustomerMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        private IntWritable nationKey = new IntWritable();
        private DoubleWritable accountBalance = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\\|");

            if (fields.length == 8) {
                int c_nationkey = Integer.parseInt(fields[3]);
                double c_acctbal = Double.parseDouble(fields[5]);

                nationKey.set(c_nationkey);
                accountBalance.set(c_acctbal);

                context.write(nationKey, accountBalance);
            }
        }
    }

    public static class CustomerReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Nation Balance Sum");

        job.setJarByClass(CustomerNationBalanceSum.class);
        job.setMapperClass(CustomerMapper.class);
        job.setReducerClass(CustomerReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
