package proj02.task03;//package KMeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		float avgX = 0;
		float avgY = 0;
		int count = 0;
		List<String[]> pointList = new ArrayList<>();

		for (Text val : value) {
			String line = val.toString();
			String[] fields = line.split(" ");
			pointList.add(fields);
			int size = Integer.parseInt(fields[0]);
			count += size;
		}

		for (String[] strings : pointList) {
			int size = Integer.parseInt(strings[0]);
			float x = Float.parseFloat(strings[1]);
			float y = Float.parseFloat(strings[2]);
			avgX += x * size / count;
			avgY += y * size / count;
		}

		Text result = new Text(avgX + " " + avgY);
		ArrayList<Float> tmpList = new ArrayList<>();
		tmpList.add((float) key.get());
		tmpList.add(avgX);
		tmpList.add(avgY);
		context.write(key, result);
	}
}
