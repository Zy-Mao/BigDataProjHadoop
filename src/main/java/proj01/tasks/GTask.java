package proj01.tasks;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GTask {
	public static class FirstMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			AccessLog accessLog = new AccessLog(value.toString());
			context.write(new IntWritable(accessLog.getByWho()), new IntWritable(accessLog.getAccessTime()));
		}
	}

	public static class FirstReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int lastAccessedTime = 1;
			int comparedTime = 990000;
			for (IntWritable val : values) {
				if (val.get() > lastAccessedTime) {
					lastAccessedTime = val.get();
				}
			}
			if (lastAccessedTime < comparedTime) {
				context.write(key, new IntWritable(lastAccessedTime));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input file path] [output file path]");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "GTask - Task g");
		job.setJarByClass(GTask.class);
		job.setMapperClass(FirstMapper.class);
		job.setReducerClass(FirstReducer.class);
//		if (args[2].equals("1")) {
//			job.setCombinerClass(FirstCombiner.class);
//			System.out.println("Combiner would be used.");
//		} else {
//			System.out.println("Combiner would not be used.");
//		}
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/"+ timeStamp));
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
			String[] strings= string.split(",");
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

		public int getByWho() {
			return byWho;
		}

		public int getAccessTime() {
			return accessTime;
		}
	}
}
