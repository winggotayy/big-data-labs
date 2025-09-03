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

    // Mapper类，用于将数据点分配到最近的中心
    public static class KMeansClusterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private static final Logger LOG = Logger.getLogger(KMeansClusterMapper.class.getName());
        private double[][] clusterCenters;  // 存储聚类中心
        private int clusterCount;           // 聚类中心的数量
        private int vectorDim;              // 向量的维度

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            clusterCount = conf.getInt("cluster.count", 20);      // 从配置中获取聚类中心的数量
            vectorDim = conf.getInt("vector.dimension", 16);      // 从配置中获取向量维度
            clusterCenters = new double[clusterCount][vectorDim]; // 初始化聚类中心数组

            URI[] cacheFiles = context.getCacheFiles(); // 获取缓存文件（包含初始聚类中心）
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path path = new Path(cacheFiles[0].toString());
                FileSystem fs = FileSystem.get(conf);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    int centerIndex = 0;
                    // 读取初始聚类中心文件
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
            // 计算每个聚类中心与数据点之间的欧几里得距离，找出最近的中心
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

    // Reducer类，用于计算新的聚类中心
    public static class KMeansClusterReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int vectorDim = context.getConfiguration().getInt("vector.dimension", 16); // 获取向量维度
            double[] newCenter = new double[vectorDim];
            int count = 0;

            // 计算聚类中心
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

            // 计算新的聚类中心
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

            context.write(key, new Text(newCenterStr.toString())); // 输出新的聚类中心
        }
    }

    // 主类，配置并运行MapReduce作业
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: KMeansCluster <input path> <output path> <initial centers path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("cluster.count", 20);     // 设置聚类中心数量
        conf.setInt("vector.dimension", 16);  // 设置向量维度

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

        job.addCacheFile(new URI(args[2])); // 添加初始聚类中心文件到缓存

        System.exit(job.waitForCompletion(true) ? 0 : 1); // 等待作业完成并退出
    }
}