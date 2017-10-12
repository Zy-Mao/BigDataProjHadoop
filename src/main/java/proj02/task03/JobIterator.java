package proj02.task03;//package KMeans;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.*;

public class JobIterator {
	//Read the ID, x, and y of every center.
    public static List<ArrayList<Float>> getCenters(String inputpath){
        List<ArrayList<Float>> result = new ArrayList<ArrayList<Float>>();
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path in = new Path(inputpath);
            FSDataInputStream fsIn = hdfs.open(in);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();
            while (lineIn.readLine(line) > 0){
                String record = line.toString();
                // Hadoop would use tab to separate the key and value.
				// Here we would use space to replace tab.
                String[] fields = record.replace("\t", " ").split(" ");
                ArrayList<Float> tmplist = new ArrayList<>();
                for (int i = 0; i < fields.length; ++i){
                    tmplist.add(Float.parseFloat(fields[i]));
                }
                result.add(tmplist);
            }
            fsIn.close();
        } catch (IOException e){
            e.printStackTrace();
        }
        return result;
    }

    //delete the result of last job
    public static void deleteLastResult(String path){
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path path1 = new Path(path);
            hdfs.delete(path1, true);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    //Calculate the distance between old center and new center
	//to determine whether it is converged.
    public static boolean isConverged(Configuration conf) throws IOException{
		String oldpath = conf.get("old.center.path");
		String newpath = conf.get("new.center.path");
		int k = Integer.parseInt(conf.get("K"));
		float threshold = Float.parseFloat(conf.get("threshold"));

        List<ArrayList<Float>> oldcenters = JobIterator.getCenters(oldpath);
        List<ArrayList<Float>> newcenters = JobIterator.getCenters(newpath);
//      for (ArrayList<Float> oldcenter : oldcenters) {
//        	System.out.println(oldcenter.get(0) + " " + oldcenter.get(1) + " " + oldcenter.get(2));
//		}
//		for (ArrayList<Float> newcenter : newcenters) {
//			System.out.println(newcenter.get(0) + " " + newcenter.get(1) + " " + newcenter.get(2));
//		}
        float distance = 0;
		for (int i = 0; i < k; ++i){
            for (int j = 1; j < oldcenters.get(i).size(); ++j){
                float tmp = Math.abs(oldcenters.get(i).get(j) - newcenters.get(i).get(j));
                distance += Math.pow(tmp, 2);
            }
        }
		distance = (float) Math.sqrt(distance);
        System.out.println("Distance = " + distance + " Threshold = " + threshold);
        return (distance < threshold);
    }

    public static void prepaerNextIteration(String oldpath, String newpath) throws IOException{
    	// Use the new center to replace the old center
		// if we need another round of calculation.
		JobIterator.deleteLastResult(oldpath);
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.copyToLocalFile(new Path(newpath), new Path("./oldcenter.data"));
		hdfs.delete(new Path(oldpath), true);
		hdfs.moveFromLocalFile(new Path("./oldcenter.data"), new Path(oldpath));
	}
}
