import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Sorter {
    public static class SorterMapper extends Mapper<Object, Text, FloatWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String split_template = "[\\s,]+";
	    String[] str_value = value.toString().split(split_template);

	    FloatWritable mykey = new FloatWritable(Float.parseFloat(str_value[1]));
	    Text val = new Text(str_value[0]) ;
	    
	    context.write(mykey, val);
        }
    }

    public static class SorterReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        @Override
        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
	       	throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }

    private static void make(Job job) throws Exception {
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Job setUp(String name) throws Exception{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, name);
        
	job.setJarByClass(Sorter.class);
        job.setMapperClass(Sorter.SorterMapper.class);
        job.setReducerClass(Sorter.SorterReducer.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
	return job;
    }
  

    public static void main(String[] args) throws Exception{

	Job mainjob = Sorter.setUp("Sorter");
        
	FileInputFormat.addInputPath(mainjob, new Path(args[0]));
        FileOutputFormat.setOutputPath(mainjob, new Path(args[1]));
        
	Sorter.make(mainjob);
    }
}
