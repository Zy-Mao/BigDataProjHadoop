package proj01.tasks;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CTask {
	public static class FirstMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private IntWritable whatPage = new IntWritable();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			String line = value.toString();
			AccessLog accessLog = new AccessLog(line);
			whatPage.set(accessLog.getWhatPage());
			context.write(whatPage, one);
		}
	}

	public static class FirstCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable count = new IntWritable();

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			count.set(sum);
			context.write(key, count);
		}
	}

	public static class FirstReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private List<Map.Entry<Integer, Integer>> pageCountList = new ArrayList<>();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			// if there are less than 10 elements in the list
			// or current elements is larger than the smallest element in the list
			// then add the new elements and remove the smallest element.
			if (pageCountList.size() < 10 || sum > pageCountList.get(9).getValue()) {
				pageCountList.add(new AbstractMap.SimpleEntry<>(key.get(), sum));
				// this comparator would make sure that the list is sorted in descending order
				Collections.sort(pageCountList, new Comparator<Map.Entry<Integer, Integer>>() {
					@Override
					public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
						return o2.getValue().compareTo(o1.getValue());
					}
				});

				if (pageCountList.size() > 10) {
					pageCountList = pageCountList.subList(0, 10);
				}
			}
		}
		// Output the top 10 key-value pairs
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			for (Map.Entry<Integer, Integer> entry : pageCountList) {
				context.write(new IntWritable(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: [input file path] [output file path] [Using combiner, 1:0]");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CTask - Task c");
		job.setJarByClass(CTask.class);
		job.setMapperClass(FirstMapper.class);
		if (args[2].equals("1")) {
			job.setCombinerClass(FirstCombiner.class);
			System.out.println("Combiner would be used.");
		} else {
			System.out.println("Combiner would not be used.");
		}
		job.setReducerClass(FirstReducer.class);
		// set number of task to 1 to make sure that all the pairs would come to one reducer
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + timeStamp));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class AccessLog {
		private int accessId;
		private int byWho;
		private int whatPage;
		private String typeOfAccess;
		private int accessTime;

		// Construct the instance according to input string.
		public AccessLog(String string) {
			String[] strings = string.split(",");
			if (strings.length == 5) {
				this.accessId = Integer.parseInt(strings[0]);
				this.byWho = Integer.parseInt(strings[1]);
				this.whatPage = Integer.parseInt(strings[2]);
				this.typeOfAccess = strings[3];
				this.accessTime = Integer.parseInt(strings[4]);
			}
		}

		@Override
		public String toString() {
			return accessId + "," + byWho + "," + whatPage + "," + typeOfAccess + "," + accessTime;
		}

		public int getWhatPage() {
			return whatPage;
		}
	}
}