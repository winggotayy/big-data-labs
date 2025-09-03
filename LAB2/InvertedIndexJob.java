import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexJob {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private final Text word = new Text();
        private final Text location = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            String[] tokens = value.toString().split("[\\s,]+");
            for (String token : tokens) {
                word.set(token);
                location.set(fileName);
                context.write(word, location);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> countMap = new HashMap<>();
            int totalCount = 0;

            for (Text val : values) {
                String fileName = val.toString();
                countMap.put(fileName, countMap.getOrDefault(fileName, 0) + 1);
                totalCount++;
            }

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%.2f", ((double) totalCount) / countMap.size()));
            sb.append(",");

            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
            }

            result.set(sb.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndexJob.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true) ;
    }
}
