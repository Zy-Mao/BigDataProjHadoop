package proj01.tasks;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DTask {
	public static class MyPageMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			MyPage myPage = new MyPage(value.toString());
			context.write(new IntWritable(myPage.getId()), new Text("P" + myPage.getName()));
		}
	}

	public static class FriendsMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Friends friends = new Friends(value.toString());
			context.write(new IntWritable(friends.getMyFriend()), new Text("F" + "1"));
		}
	}

	public static class FirstCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				if (val.toString().startsWith("P")) {
					// if value starts with "P", then send it to reducer ahead
					context.write(key, val);
				} else if (val.toString().startsWith("F")) {
					// otherwise count the value
					sum += Integer.parseInt(val.toString().substring(1));
				}
			}
			context.write(key, new Text("F" + String.valueOf(sum)));
		}
	}

	public static class FirstReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			String pageName = "";
			for (Text val : values) {
				if (val.toString().startsWith("P")) {
					// if value starts with "P", then set it as the page name
					pageName = val.toString().substring(1);
				} else if (val.toString().startsWith("F")) {
					// otherwise count the value
					sum += Integer.parseInt(val.toString().substring(1));
				}
			}
			// output the join result
			context.write(new Text(pageName), new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("Usage: [input MyPage file path] [input Friends file path] [output file path] [Using combiner, 1:0]");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DTask - Task d");
		job.setJarByClass(DTask.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		if (args[3].equals("1")) {
			job.setCombinerClass(FirstCombiner.class);
			System.out.println("Combiner would be used.");
		} else {
			System.out.println("Combiner would not be used.");
		}
		job.setReducerClass(FirstReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, MyPageMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, FriendsMapper.class);
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		FileOutputFormat.setOutputPath(job, new Path(args[2] + "/" + timeStamp));
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

	public static class MyPage {
		private int id;
		private String name;
		private String nationality;
		private int countryCode;
		private String hobby;

		// Construct the instance according to input string.
		public MyPage(String string) {
			String[] strings= string.split(",");
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

		public int getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}
}