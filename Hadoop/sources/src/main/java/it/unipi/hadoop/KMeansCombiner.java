package it.unipi.hadoop;

import it.unipi.hadoop.datastr.CentroidWritable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 */
public class KMeansCombiner extends Reducer<CentroidWritable, CentroidWritable, CentroidWritable, CentroidWritable> {
    
    private CentroidWritable localAvgCentroid;
    
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        int dim = context.getConfiguration().getInt("dim", 3);
        localAvgCentroid = new CentroidWritable(new float[dim], dim);
    }
    
    @Override
    protected void reduce(CentroidWritable originalCentroid, Iterable<CentroidWritable> points, Context context) throws IOException, InterruptedException {
        Iterator<CentroidWritable> iterator = points.iterator();
        
        if(!iterator.hasNext())
            return;
        
        int count = 0;
        localAvgCentroid.resetToZero();
        while(iterator.hasNext()) {
            localAvgCentroid.sum(iterator.next());
            ++count;
        }
        
        localAvgCentroid.divideCoordsBy((float)count);
        localAvgCentroid.setWeight(count);
        
        context.write(originalCentroid,localAvgCentroid);
    }
    
}
