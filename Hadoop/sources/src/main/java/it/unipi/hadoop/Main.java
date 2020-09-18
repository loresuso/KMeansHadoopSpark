package it.unipi.hadoop;

import it.unipi.hadoop.datastr.CentroidWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    private static final float THRESHOLD = 0.1f;
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        if (args.length != 5) {
           System.err.println("Usage: KMeans <dim> <k> <centroidList> <inputFilePath> <outputDir>");
           System.exit(1);
        }
        
        int dim = Integer.parseInt(args[0]);
        int k = Integer.parseInt(args[1]);
        
        System.out.println("args[0]: dim=" + dim);
        System.out.println("args[1]: k=" + k);
        System.out.println("args[2]: centroidList=" + args[2]);
        System.out.println("args[3]: inputFilePath=" + args[3]);
        System.out.println("args[4]: outputDir=" + args[4]);
        
        Path inPath = new Path(args[3]);
        Path outPath = new Path(args[4]);
        
        long startTime = System.currentTimeMillis();
        
        conf.setInt("dim", dim);
        conf.setInt("k", k);
        Utils.writeCentroidListToConf(conf, args[2]);
        
        CentroidWritable[] newCentroids;
        
        while(true) {
            CentroidWritable[] oldCentroids = Utils.readCentroidsFromConf(conf);
            
            Job job = Job.getInstance(conf, "Main");
            job.setJarByClass(Main.class);
            // set mapper/combiner/reducer
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            //job.setNumReduceTasks(13);

            // define mapper's output key-value
            job.setMapOutputKeyClass(CentroidWritable.class);
            job.setMapOutputValueClass(CentroidWritable.class);
            
            // define reducer's output key-value
            job.setOutputKeyClass(CentroidWritable.class);
            job.setOutputValueClass(Object.class);

            // define I/O
            FileInputFormat.addInputPath(job, inPath);
            FileOutputFormat.setOutputPath(job, outPath);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            if(!job.waitForCompletion(true)) {
                System.err.println("Job failed!!!");
                System.exit(1);
            }
            
            //Set the new centroids
            newCentroids = Utils.readCentroidsFromHdfs(conf,args[4],dim);
            Utils.writeCentroidsToConf(conf, newCentroids);
            
            if(converging(oldCentroids, newCentroids)) {
                break;
            }
            
            Utils.deleteFromHdfs(conf, outPath);
        }
        
        for(CentroidWritable centroid : newCentroids) {
            System.out.println("Centroid: " + centroid.toString());
        }
        
        long endTime = System.currentTimeMillis();
        System.out.println(String.format(
                "Completed after %.3f seconds", ((float)(endTime-startTime)/1000.0f)
        ));
     }
    
    private static boolean converging(CentroidWritable[] a, CentroidWritable[] b) {
        if(a.length != b.length) {
            /*throw new RuntimeException(
                "converging(...) got a different number of centroid dims\n" +
                "a.lenght = " + a.length + " | b.lenght = " + b.length
            );*/
            return false;
        }
        for(int i = 0; i < a.length; ++i) {
            if(a[i].distanceTo(b[i]) > THRESHOLD) {
                return false;
            }
        }
        return true;
    }
    
}