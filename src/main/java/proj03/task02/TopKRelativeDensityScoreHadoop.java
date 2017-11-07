package proj03.task02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by mao on 10/26/17.
 */
public class TopKRelativeDensityScoreHadoop {
	public static class FirstMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String stringSplits[] = value.toString().split(",");
			if (stringSplits.length != 2) return;
			IntWritable cellId = new IntWritable(getGridCellId(Integer.parseInt(stringSplits[0]),
					Integer.parseInt(stringSplits[1])));
			context.write(cellId, new IntWritable(1));
		}
		private int getGridCellId(int x, int y) {
			return (x - 1) / 20 + ((10000 - y) / 20) * 500;
		}
	}

	public static class FirstReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static class SecondMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String stringSplits[] = value.toString().split("/t");
			if (stringSplits.length != 2) return;
			int cellId = Integer.parseInt(stringSplits[0]);
			int count = Integer.parseInt(stringSplits[1]);
			context.write(new IntWritable(cellId), new Text("S" + String.valueOf(count)));
			for (int i = (cellId % 500 == 1 ? 0 : -1); i <= (cellId % 500 == 0 ? 0 : 1); i++) {
				for (int j = (cellId <= 500 ? 0 : -1); j <= (cellId >= 24950 ? 0 : 1); j++) {
					if (i == 0 && j == 0) {
						continue;
					}
					int neighborId = cellId + i + j * 500;
					context.write(new IntWritable(neighborId), new Text("N" + String.valueOf(count)));
				}
			}
		}
	}

	public static class SecondReducer extends Reducer<IntWritable, Text, IntWritable, FloatWritable> {
		private Map<Float, Integer> gridCellMap = new TreeMap<>(Collections.reverseOrder());

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int selfDensity = 0;
			int neighborDensity = 0;
			int neighborCount = 0;
			for (Text val : values) {
				if (val.toString().startsWith("S")) {
					selfDensity += Integer.parseInt(val.toString().substring(1));
				}
				if (val.toString().startsWith("N")) {
					neighborDensity += Integer.parseInt(val.toString().substring(1));
					neighborCount += 1;
				}
			}
			gridCellMap.put((float) selfDensity / (neighborDensity / neighborCount), key.get());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input file path] [output file path]");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopKRelativeDensityScore - 1");
		job.setJarByClass(TopKRelativeDensityScoreHadoop.class);
		job.setMapperClass(FirstMapper.class);
		job.setCombinerClass(FirstReducer.class);
		job.setReducerClass(FirstReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/"+ timeStamp + "/tmp"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		conf = new Configuration();
		job = Job.getInstance(conf, "TopKRelativeDensityScore - 2");
		job.setJarByClass(TopKRelativeDensityScoreHadoop.class);
		job.setMapperClass(SecondMapper.class);
//		job.setCombinerClass(SecondC.class);
		job.setReducerClass(SecondReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1] + "/"+ timeStamp + "/tmp"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/"+ timeStamp));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
