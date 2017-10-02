package proj01.tasks;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BTask {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private IntWritable country = new IntWritable();

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			MyPage myPage = new MyPage(line);
			country.set(myPage.getCountryCode());
			output.collect(country, one);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: [input file path] [output file path] [Using combiner, 1:0]");
			System.exit(1);
		}

		JobConf conf = new JobConf(BTask.class);
		conf.setJobName("BTask - Task b");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		if (args[2].equals("1")) {
			// Here we could just reuse Reducer class as Combiner.
			conf.setCombinerClass(Reduce.class);
			System.out.println("Combiner would be used.");
		} else {
			System.out.println("Combiner would not be used.");
		}
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/" + timeStamp));

		JobClient.runJob(conf);
	}

	public static class MyPage {
		private int id;
		private String name;
		private String nationality;
		private int countryCode;
		private String hobby;

		// Construct the instance according to input string.
		public MyPage(String string) {
			String[] strings = string.split(",");
			if (strings.length == 5) {
				this.id = Integer.parseInt(strings[0]);
				this.name = strings[1];
				this.nationality = strings[2];
				this.countryCode = Integer.parseInt(strings[3]);
				this.hobby = strings[4];
			}
		}

		@Override
		public String toString() {
			return id + "," + name + "," + nationality + "," + countryCode + "," + hobby;
		}

		public int getCountryCode() {
			return countryCode;
		}
	}
}
