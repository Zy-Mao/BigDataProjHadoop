package proj01.tasks;

import proj01.entity.*;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ETask {
	public static class FirstMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			AccessLog accessLog = new AccessLog(value.toString());
			context.write(new IntWritable(accessLog.getByWho()),
					new Text(String.valueOf(accessLog.getWhatPage()) + "_" + "1"));
		}
	}

	public static class FirstCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<Integer, Integer> hashMap = new HashMap<>();
			for (Text val : values) {
				String[] strings = val.toString().split("_");
				if (strings.length == 2) {
					int whatPage = Integer.parseInt(strings[0]);
					int count = Integer.parseInt(strings[1]);
					int prevCount = hashMap.containsKey(whatPage) ? hashMap.get(whatPage) : 0;
					hashMap.put(whatPage, prevCount + count);
				}
			}
			for (Map.Entry<Integer, Integer> entry : hashMap.entrySet()) {
				context.write(key, new Text(String.valueOf(entry.getKey()) + "_" + String.valueOf(entry.getValue())));
			}
		}
	}

	public static class FirstReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private HashSet<Integer> whatPageSet;

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			whatPageSet = new HashSet<>();
			int sum = 0;
			for (Text val : values) {
				String[] strings = val.toString().split("_");
				if (strings.length == 2) {
					sum += Integer.parseInt(strings[1]);
					whatPageSet.add(Integer.parseInt(strings[0]));
				}
			}
			context.write(key, new Text(String.valueOf(sum) + " " + String.valueOf(whatPageSet.size())));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: [input file path] [output file path] [Using combiner, 1:0]");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ETask - Task e");
		job.setJarByClass(ETask.class);
		job.setMapperClass(FirstMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		if (args[2].equals("1")) {
			job.setCombinerClass(FirstCombiner.class);
			System.out.println("Combiner would be used.");
		} else {
			System.out.println("Combiner would not be used.");
		}
		job.setReducerClass(FirstReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/"+ timeStamp));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
