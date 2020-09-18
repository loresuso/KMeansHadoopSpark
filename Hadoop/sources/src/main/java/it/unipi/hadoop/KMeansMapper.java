package it.unipi.hadoop;

import it.unipi.hadoop.datastr.CentroidWritable;
import it.unipi.hadoop.datastr.PointWritable;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 */
public class KMeansMapper extends Mapper<LongWritable, Text, CentroidWritable, CentroidWritable> {
    
    private CentroidWritable[] centroids;
    private float[] distances;
    private PointWritable point;
    private CentroidWritable pointAsCentroid;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        centroids = Utils.readCentroidsFromConf(context.getConfiguration());
        distances = new float[centroids.length];
        int dim = context.getConfiguration().getInt("dim", 3);
        point = new PointWritable(new float[dim], dim);
        pointAsCentroid = new CentroidWritable(new float[dim], dim);
        pointAsCentroid.setWeight(1);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        if(record == null || record.length() == 0)
            return;

        String[] lines = record.split("\n");
        
        for(String line : lines) {
            point.updateFromLine(line);
            int minIndex = 0;
            float minDistance = Float.MAX_VALUE;
            for(int i = 0; i < distances.length; ++i) {
                distances[i] = centroids[i].distanceTo(point);
                if(distances[i] < minDistance) {
                    minDistance = distances[i];
                    minIndex = i;
                }
            }
            pointAsCentroid.linkTo(point);
            context.write(centroids[minIndex], pointAsCentroid);
        }
    }
    
}
