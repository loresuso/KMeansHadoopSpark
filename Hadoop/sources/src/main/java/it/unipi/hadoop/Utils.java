package it.unipi.hadoop;

import it.unipi.hadoop.datastr.CentroidWritable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class Utils {
    
    public static void writeCentroidListToConf(Configuration conf, String centroidList) {
        String temp = centroidList.replaceAll(",", "#").replaceAll(";", "!");
        conf.setStrings("controidList", temp);
    }
    
    public static void writeCentroidsToConf(Configuration conf, CentroidWritable[] centroids) {
        writeCentroidListToConf(
                conf,
                centroidsToCentroidList(centroids)
        );
    }
    
    public static CentroidWritable[] readCentroidsFromConf(Configuration conf) {
        String[] centroidsStr = conf.getStrings("controidList")[0].split("!");
        ArrayList<CentroidWritable> ret = new ArrayList();
        for(String centroidStr : centroidsStr) {
            centroidStr = centroidStr.replaceAll("#", ",");
            CentroidWritable centroid = new CentroidWritable(centroidStr);
            System.out.println("Dalla conf ho letto il centroide: " + centroid.toString());
            ret.add(centroid);
        }
        return ret.toArray(new CentroidWritable[0]);
    }
    
    public static CentroidWritable[] readCentroidsFromHdfs(Configuration conf, String pathStr, int dim) throws IOException {
        Path path = new Path(pathStr + "/part-r-00000");
        FileSystem hdfs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(path)));
        ArrayList<CentroidWritable> ret = new ArrayList();
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.replaceAll("\n", "");
            CentroidWritable centroid = new CentroidWritable(line);
            ret.add(centroid);
        }
        return ret.toArray(new CentroidWritable[0]);
    }
    
    public static void deleteFromHdfs(Configuration conf, Path path) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        hdfs.delete(path,true);
    }
    
    public static String centroidsToCentroidList(CentroidWritable... centroids) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < centroids.length; ++i) {
            sb.append(centroids[i].toString());
            if(i < centroids.length - 1)
                sb.append(";");
        }
        return sb.toString();
    }
    
}
