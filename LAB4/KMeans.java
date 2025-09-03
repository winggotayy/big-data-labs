import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.logging.Logger;

public class KMeans {

    // Mapper�࣬���ڽ����ݵ���䵽���������
    public static class KMeansClusterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private static final Logger LOG = Logger.getLogger(KMeansClusterMapper.class.getName());
        private double[][] clusterCenters;  // �洢��������
        private int clusterCount;           // �������ĵ�����
        private int vectorDim;              // ������ά��

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            clusterCount = conf.getInt("cluster.count", 20);      // �������л�ȡ�������ĵ�����
            vectorDim = conf.getInt("vector.dimension", 16);      // �������л�ȡ����ά��
            clusterCenters = new double[clusterCount][vectorDim]; // ��ʼ��������������

            URI[] cacheFiles = context.getCacheFiles(); // ��ȡ�����ļ���������ʼ�������ģ�
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path path = new Path(cacheFiles[0].toString());
                FileSystem fs = FileSystem.get(conf);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    int centerIndex = 0;
                    // ��ȡ��ʼ���������ļ�
                    while ((line = reader.readLine()) != null && centerIndex < clusterCount) {
                        String[] parts = line.split(": ");
                        if (parts.length == 2) {
                            String[] vectorParts = parts[1].split(",");
                            for (int i = 0; i < vectorDim; i++) {
                                clusterCenters[centerIndex][i] = Double.parseDouble(vectorParts[i]);
                            }
                            centerIndex++;
                        }
                    }
                }
            } else {
                throw new IOException("Cluster centers file not found in DistributedCache");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(": ");
            if (parts.length == 2) {
                String[] vectorParts = parts[1].split(",");
                double[] vector = Arrays.stream(vectorParts).mapToDouble(Double::parseDouble).toArray();
                int nearestCenter = findNearestCenter(vector);
                context.write(new IntWritable(nearestCenter), value);
            }
        }

        private int findNearestCenter(double[] vector) {
            double minDistance = Double.MAX_VALUE;
            int nearestCenterIndex = -1;
            // ����ÿ���������������ݵ�֮���ŷ����þ��룬�ҳ����������
            for (int i = 0; i < clusterCount; i++) {
                double distance = euclideanDistance(vector, clusterCenters[i]);
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCenterIndex = i;
                }
            }
            return nearestCenterIndex;
        }

        private double euclideanDistance(double[] a, double[] b) {
            double sum = 0;
            for (int i = 0; i < a.length; i++) {
                sum += Math.pow(a[i] - b[i], 2);
            }
            return Math.sqrt(sum);
        }
    }

    // Reducer�࣬���ڼ����µľ�������
    public static class KMeansClusterReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int vectorDim = context.getConfiguration().getInt("vector.dimension", 16); // ��ȡ����ά��
            double[] newCenter = new double[vectorDim];
            int count = 0;

            // �����������
            for (Text value : values) {
                String[] parts = value.toString().split(": ");
                if (parts.length == 2) {
                    String[] vectorParts = parts[1].split(",");
                    for (int i = 0; i < vectorDim; i++) {
                        newCenter[i] += Double.parseDouble(vectorParts[i]);
                    }
                    count++;
                }
            }

            // �����µľ�������
            for (int i = 0; i < vectorDim; i++) {
                newCenter[i] /= count;
            }

            StringBuilder newCenterStr = new StringBuilder();
            for (int i = 0; i < vectorDim; i++) {
                if (i > 0) {
                    newCenterStr.append(",");
                }
                newCenterStr.append(newCenter[i]);
            }

            context.write(key, new Text(newCenterStr.toString())); // ����µľ�������
        }
    }

    // ���࣬���ò�����MapReduce��ҵ
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: KMeansCluster <input path> <output path> <initial centers path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("cluster.count", 20);     // ���þ�����������
        conf.setInt("vector.dimension", 16);  // ��������ά��

        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansClusterMapper.class);
        job.setReducerClass(KMeansClusterReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(args[2])); // ��ӳ�ʼ���������ļ�������

        System.exit(job.waitForCompletion(true) ? 0 : 1); // �ȴ���ҵ��ɲ��˳�
    }
}