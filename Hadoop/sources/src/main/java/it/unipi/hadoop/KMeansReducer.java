package it.unipi.hadoop;

import it.unipi.hadoop.datastr.CentroidWritable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 */
public class KMeansReducer extends Reducer<CentroidWritable, CentroidWritable, CentroidWritable, Object> {
    
    private CentroidWritable globalAvgCentroid;
    
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int dim = context.getConfiguration().getInt("dim", 3);
        globalAvgCentroid = new CentroidWritable(new float[dim], dim);
    }
    
    @Override
    protected void reduce(CentroidWritable originalCentroid, Iterable<CentroidWritable> localAvgCentroids, Context context) throws IOException, InterruptedException {
        Iterator<CentroidWritable> iterator = localAvgCentroids.iterator();
        globalAvgCentroid.resetToZero();
        int weightSum = 0;
        float[] coords = globalAvgCentroid.getCoords();
        while(iterator.hasNext()) {
            CentroidWritable localAvgCentroid = iterator.next();
            float[] lvcCoords = localAvgCentroid.getCoords();
            for(int i = 0; i < coords.length; ++i) {
                coords[i] += lvcCoords[i] * (float)localAvgCentroid.getWeight();
            }
            weightSum += localAvgCentroid.getWeight();
        }
        
        if(weightSum == 0)
            return;
        
        globalAvgCentroid.divideCoordsBy((float)weightSum);
        
        context.write(globalAvgCentroid,null);
    }
    
}
