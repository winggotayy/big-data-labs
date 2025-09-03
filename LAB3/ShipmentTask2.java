import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class ShipmentTask2 {
  static Boolean ready;

  public static abstract class KeyPriorityPair_t<Key_t extends WritableComparable<? super Key_t>, Val_t extends WritableComparable<? super Val_t> >
      implements WritableComparable<KeyPriorityPair_t<Key_t, Val_t> > {
    private Key_t key;
    private Val_t value;

    public void setKey(Key_t key) {
      this.key = key;
    }

    public void setValue(Val_t value) {
      this.value = value;
    }

    public Key_t getKey() {
      return this.key;
    }

    public Val_t getValue() {
      return this.value;
    }

    public KeyPriorityPair_t(Key_t key, Val_t value) {
      this.key = key;
      this.value = value;
    }

    public int compareTo(KeyPriorityPair_t<Key_t, Val_t> another) {
      int result = this.key.compareTo(another.getKey());
      if (result == 0) {
        return this.value.compareTo(another.getValue());
      } else {
        return result;
      }
    }

    public void write(DataOutput ostream) throws IOException {
      this.key.write(ostream);
      this.value.write(ostream);
    }

    public void readFields(DataInput istream) throws IOException {
      this.key.readFields(istream);
      this.value.readFields(istream);
    }
  }

  public static class MyPair_t extends KeyPriorityPair_t<Text, IntWritable> {
    public MyPair_t(Text text, IntWritable intWritable) {
      super(text, intWritable);
    }

    public MyPair_t() {
      this(new Text(), new IntWritable());
    }
  }

  public static class Mapper_t extends Mapper<Object, Text, MyPair_t, Text> {
    private static String separator = "|";

    public static String[] extractFields(String str) {
      // extracts fields from a line
      
      if (str == null) {
        throw new IllegalArgumentException("Arguments cannot be null or empty");
      }
    
      int capacity = 0;
      int start = 0;
      int end;
    
      // Count the number of splits based on the separator
      while ((end = str.indexOf(separator, start)) != -1) {
        capacity++;
        start = end + 1;
      }
    
      // Add one for the last split (assuming the separator isn't at the end)
      if (start < str.length()) {
        capacity++;
      }
    
      // Create the result array and populate it
      String[] result = new String[capacity];
      capacity = 0;
      start = 0;
      while ((end = str.indexOf(separator, start)) != -1) {
        result[capacity++] = str.substring(start, end);
        start = end + separator.length();
      }
    
      // Add the last part (if it exists)
      if (start < str.length()) {
        result[capacity++] = str.substring(start);
      }
      String[] fields = new String[3];
      fields[0] = result[0];
      fields[1] = result[5];
      fields[2] = result[7];
    
      return fields;
    }

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] fieldsList = extractFields(value.toString());
      Text orderKey = new Text(fieldsList[1]);
      IntWritable orderPriority = new IntWritable(-Integer.parseInt(fieldsList[2]));
      MyPair_t priorityKeyPair = 
        new MyPair_t(
          orderKey,
          orderPriority
        );

      context.write(
        priorityKeyPair,
        new Text(fieldsList[0]));
    }
  }

  public static class Partitioner_t extends Partitioner<MyPair_t, Text> {
    private final HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<>();

    @Override
    public int getPartition(MyPair_t myPair, Text text, int i) {
      return hashPartitioner.getPartition(myPair.getKey(), text, i);
    }
  }

  public static class Reducer_t extends Reducer<MyPair_t, Text, Text, NullWritable> {
    private String elem;

    private static class Desirializer_t {
      private static Text deserialize(Text value, String key_str, int shipPrior) {
        String text = value.toString() + key_str + String.format("%d", shipPrior);
        return new Text(text);
      }
    }

    @Override
    public void reduce(MyPair_t key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      int shipPrior = key.getValue().get();
      String key_str = key.getKey().toString();

      if (this.elem.equals(key_str) == false) {
        this.elem = key_str;

        for (Text t : values) {
          context.write(
              Desirializer_t.deserialize(t, key_str, shipPrior),
              NullWritable.get()
          );
        }
      }
    }
    
    @Override
    public void setup(Context c) {
      this.elem = new String("");
    }

  }

  public static void exec(Job job) throws Exception{
      if (ShipmentTask2.ready) {
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
  }

  public static class JobBuilder {
    private String name;
    private Configuration conf;

    public JobBuilder() {
      this.conf = new Configuration();
      this.name = new String();
    }

    public JobBuilder setName(String name) {
      this.name = name;
      return this;
    }

    public JobBuilder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Job build() throws IOException {
      Job newJob = Job.getInstance(this.conf, this.name);

      newJob.setMapperClass(ShipmentTask2.Mapper_t.class);
      newJob.setPartitionerClass(ShipmentTask2.Partitioner_t.class);
      newJob.setReducerClass(ShipmentTask2.Reducer_t.class);

      newJob.setJarByClass(ShipmentTask2.class);

      newJob.setMapOutputKeyClass(ShipmentTask2.MyPair_t.class);
      newJob.setMapOutputValueClass(Text.class);

      newJob.setOutputKeyClass(Text.class);
      newJob.setOutputValueClass(NullWritable.class);

      return newJob;
    }
  }

  public static void main(String[] args) throws Exception {
    ShipmentTask2.ready = false;

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    JobBuilder builder = new JobBuilder();
    Job mainJob = builder.setName("ShipmentTask2").build();

    FileInputFormat.addInputPath(mainJob, inputPath);
    FileOutputFormat.setOutputPath(mainJob, outputPath);

    ShipmentTask2.ready = true;
    exec(mainJob);
  }
}
