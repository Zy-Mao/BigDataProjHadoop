package proj01.tasks;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ATask {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text name = new Text();
		private Text hobby = new Text();

		// Convert input string into MyPage instance and output if the nationality is satisfied.
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			MyPage myPage = new MyPage(line);
			if (myPage.getNationality().equals("China")) {
				name.set(myPage.getName());
				hobby.set(myPage.getHobby());
				output.collect(name, hobby);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input file path] [output file path]");
			System.exit(1);
		}

		JobConf conf = new JobConf(ATask.class);
		conf.setJobName("ATask - Task a");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		// No reduce task needed, no combiner needed.
		conf.setMapperClass(Map.class);
		conf.setNumReduceTasks(0);

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

		public String getName() {
			return name;
		}

		public String getNationality() {
			return nationality;
		}

		public String getHobby() {
			return hobby;
		}
	}

}
