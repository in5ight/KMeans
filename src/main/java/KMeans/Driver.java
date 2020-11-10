package KMeans;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[0]);
        int iterationNum = Integer.parseInt(args[1]);
        String sourcePath = args[2];
        String outputPath = args[3];

        GenerateInitialClusters generateInitialClusters = new GenerateInitialClusters(conf, sourcePath, k);
        generateInitialClusters.generate(outputPath + "/");

        for (int i = 0; i < iterationNum; i++) {
            Job clusterCenterJob = Job.getInstance(conf);
            clusterCenterJob.setJobName("clusterCenterJob" + i);
            clusterCenterJob.setJarByClass(KMeansIteration.class);
            clusterCenterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i + "/");

            clusterCenterJob.setMapperClass(KMeansIteration.KMeansIterationMapper.class);
            clusterCenterJob.setMapOutputKeyClass(IntWritable.class);
            clusterCenterJob.setMapOutputValueClass(Cluster.class);

            clusterCenterJob.setCombinerClass(KMeansIteration.KMeansIterationCombiner.class);

            clusterCenterJob.setReducerClass(KMeansIteration.KMeansIterationReducer.class);
            clusterCenterJob.setOutputKeyClass(NullWritable.class);
            clusterCenterJob.setOutputValueClass(Cluster.class);

            FileInputFormat.addInputPath(clusterCenterJob, new Path(sourcePath));
            FileOutputFormat.setOutputPath(clusterCenterJob, new Path(outputPath + "/cluster-" + (i + 1) + "/"));

            try {
                clusterCenterJob.waitForCompletion(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Job kMeansClusterJob = Job.getInstance(conf);
        kMeansClusterJob.setJobName("KMeansClusterJob");
        kMeansClusterJob.setJarByClass(KMeansFinalClusters.class);
        kMeansClusterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + (iterationNum - 1) + "/");
        kMeansClusterJob.setMapperClass(KMeansFinalClusters.KMeansFinalClustersMapper.class);
        kMeansClusterJob.setMapOutputKeyClass(Text.class);
        kMeansClusterJob.setMapOutputValueClass(IntWritable.class);
        kMeansClusterJob.setNumReduceTasks(0);

        FileInputFormat.addInputPath(kMeansClusterJob, new Path(sourcePath));
        FileOutputFormat.setOutputPath(kMeansClusterJob, new Path(outputPath + "/points" + "/"));

        try {
            kMeansClusterJob.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}