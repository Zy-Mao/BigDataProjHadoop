package proj02.task03;//package KMeans;
//gai
//import KMeans.JobIterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
	private JobIterator A;
	private List<ArrayList<Float>> centers;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.A = new JobIterator();
		this.centers = A.getCenters(context.getConfiguration().get("old.center.path"));
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split(" ");

		int k = Integer.parseInt(context.getConfiguration().get("K"));
		float minDist = Float.MAX_VALUE;
		int centerIndex = 0;

		float currentX = Float.parseFloat(fields[0]);
		float currentY = Float.parseFloat(fields[1]);
		float minCenterX = 0;
		float minCenterY = 0;
		if (currentX > 10000 || currentY > 10000) {
			System.out.println("Mapper " + currentX + " " + currentY);
			System.out.println("Mapper " + value.toString());
		}
		for (int i = 0; i < k && i < centers.size(); ++i) {
			float currentDist = 0;
			float centerID = centers.get(i).get(0);
			float centerX = centers.get(i).get(1);
			float centerY = centers.get(i).get(2);
			float tmp = Math.abs(centerX - currentX);
			currentDist += Math.pow(tmp, 2);
			tmp = Math.abs(centerY - currentY);
			currentDist += Math.pow(tmp, 2);
			// calculate the distance and cluster point to the nearest center.
			if (minDist > currentDist) {
				minDist = currentDist;
				centerIndex = Math.round(centerID);
				minCenterX = centerX;
				minCenterY = centerY;
			}
		}
		String outputString = "";
		if (context.getConfiguration().get("include.center.in.mapper").equals("1")) {
			// check if we need to put center in the output file.
			outputString = value.toString() + "\tcenter: " + minCenterX + "\t" + minCenterY;
		} else {
			outputString = "1" + " " + value.toString();
		}
		context.write(new IntWritable(centerIndex), new Text(outputString));
	}
}
