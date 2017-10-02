package proj01.tasks;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HTask {

	public static class FirstMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Friends friends = new Friends(line);
			context.write(new IntWritable(friends.getMyFriend()), new IntWritable(1));
		}
	}

	public static class FirstCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class FirstReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private Map<Integer, Integer> friendsCountMap;
		private int totalRecords;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.totalRecords = 0;
			this.friendsCountMap = new TreeMap<>(Collections.reverseOrder());
		}

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				totalRecords += val.get();
			}
			friendsCountMap.put(key.get(), sum);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			int average = totalRecords / friendsCountMap.size();
			for (Map.Entry<Integer, Integer> entry : friendsCountMap.entrySet()) {
				if (entry.getValue() > average) {
					context.write(new IntWritable(entry.getKey()),
							new IntWritable(entry.getValue()));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: [input file path] [output file path] [Using combiner, 1:0]");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HTask - Task h");
		job.setJarByClass(HTask.class);
		job.setMapperClass(FirstMapper.class);
		if (args[2].equals("1")) {
			job.setCombinerClass(FirstCombiner.class);
			System.out.println("Combiner would be used.");
		} else {
			System.out.println("Combiner would not be used.");
		}
		job.setReducerClass(FirstReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + timeStamp));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Friends {
		private int friendRel;
		private int personId;
		private int myFriend;
		private int dateOfFriendship;
		private String desc;

		// Construct the instance according to input string.
		public Friends(String string) {
			String[] strings= string.split(",");
			if (strings.length == 5) {
				this.friendRel = Integer.parseInt(strings[0]);
				this.personId = Integer.parseInt(strings[1]);
				this.myFriend = Integer.parseInt(strings[2]);
				this.dateOfFriendship = Integer.parseInt(strings[3]);
				this.desc = strings[4];
			}
		}

		@Override
		public String toString() {
			return friendRel + "," + personId + "," + myFriend + "," + dateOfFriendship + "," + desc;
		}

		public int getMyFriend() {
			return myFriend;
		}
	}
}