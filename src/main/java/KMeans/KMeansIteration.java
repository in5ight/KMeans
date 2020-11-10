package KMeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansIteration {
    public static class KMeansIterationMapper extends Mapper<LongWritable, Text, IntWritable, Cluster> {
        private ArrayList<Cluster> kClusters = new ArrayList<Cluster>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FileStatus[] fileList = fs.listStatus(new Path(context.getConfiguration().get("clusterPath")));
            BufferedReader in = null;
            FSDataInputStream fsi = null;
            String line = null;
            for (int i = 0; i < fileList.length; i++) {
                if (!fileList[i].isDirectory()) {
                    fsi = fs.open(fileList[i].getPath());
                    in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
                    while ((line = in.readLine()) != null) {
                        Cluster cluster = new Cluster(line);
                        cluster.setNumOfPoints(0);
                        kClusters.add(cluster);
                    }
                }
            }
            in.close();
            fsi.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Point pnt = new Point(value.toString());
            int id;
            try {
                id = getNearest(pnt);
                if (id == -1)
                    throw new InterruptedException("id == -1");
                else {
                    Cluster cluster = new Cluster(id, pnt);
                    cluster.setNumOfPoints(1);
                    context.write(new IntWritable(id), cluster);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public int getNearest(Point pnt) throws Exception {
            int id = -1;
            double distance = Double.MAX_VALUE;
            double newDis = 0.0;
            for (Cluster cluster : kClusters) {
                double x1 = cluster.getCenter().getx();
                double y1 = cluster.getCenter().gety();
                double x2 = pnt.getx();
                double y2 = pnt.gety();
                newDis = Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
                if (newDis < distance) {
                    id = cluster.getClusterID();
                    distance = newDis;
                }
            }
            return id;
        }

        public Cluster getClusterByID(int id) {
            for (Cluster cluster : kClusters) {
                if (cluster.getClusterID() == id)
                    return cluster;
            }
            return null;
        }
    }

    public static class KMeansIterationCombiner extends Reducer<IntWritable, Cluster, IntWritable, Cluster> {
        public void reduce(IntWritable key, Iterable<Cluster> value, Context context)
                throws IOException, InterruptedException {
            Point pnt = new Point();
            int numOfPoints = 0;
            for (Cluster cluster : value) {
                numOfPoints += cluster.getNumOfPoints();
                pnt = pnt.add(cluster.getCenter().multiply(cluster.getNumOfPoints()));
            }
            Cluster cluster = new Cluster(key.get(), pnt.divide(numOfPoints));
            cluster.setNumOfPoints(numOfPoints);
            context.write(key, cluster);
        }
    }

    public static class KMeansIterationReducer extends Reducer<IntWritable, Cluster, NullWritable, Cluster> {
        public void reduce(IntWritable key, Iterable<Cluster> value, Context context)
                throws IOException, InterruptedException {
            Point pnt = new Point();
            int numOfPoints = 0;
            for (Cluster cluster : value) {
                numOfPoints += cluster.getNumOfPoints();
                pnt = pnt.add(cluster.getCenter().multiply(cluster.getNumOfPoints()));
            }
            Cluster cluster = new Cluster(key.get(), pnt.divide(numOfPoints));
            cluster.setNumOfPoints(numOfPoints);
            context.write(NullWritable.get(), cluster);
        }
    }
}