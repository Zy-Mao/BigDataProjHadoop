package proj01.tasks;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FTask {
	public static class FriendsMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Friends friends = new Friends(value.toString());
			context.write(new IntWritable(friends.getPersonId()),
					new Text("F" + String.valueOf(friends.getMyFriend())));
		}
	}

	public static class AccessLogMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			AccessLog accessLog = new AccessLog(value.toString());
			context.write(new IntWritable(accessLog.getByWho()),
					new Text("A" + String.valueOf(accessLog.getWhatPage())));
		}
	}

	public static class FirstCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
		private HashSet<Integer> accessLogSet;

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			accessLogSet = new HashSet<>();
			for (Text val : values) {
				// if val start with friend, then send to the reducer directly.
				if (val.toString().startsWith("F")) {
					context.write(key, val);
				} else if (val.toString().startsWith("A")) {
					accessLogSet.add(Integer.parseInt(val.toString().substring(1)));
				}
			}
			for (Integer i : accessLogSet) {
				context.write(key, new Text("A" + String.valueOf(i)));
			}
		}
	}

	public static class FirstReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
		private HashSet<Integer> friendsSet;
		private HashSet<Integer> accessLogSet;
		private HashSet<Integer> resultSet;

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			friendsSet = new HashSet<>();
			accessLogSet = new HashSet<>();
			resultSet = new HashSet<>();
			for (Text val : values) {
				if (val.toString().startsWith("F")) {
					friendsSet.add(Integer.parseInt(val.toString().substring(1)));
				} else if (val.toString().startsWith("A")) {
					accessLogSet.add(Integer.parseInt(val.toString().substring(1)));
				}
			}
			for (Integer i : friendsSet) {
				if (!accessLogSet.contains(i)) {
					resultSet.add(i);
				}
			}
			for (Integer i : resultSet) {
				context.write(key, new IntWritable(i));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("Usage: [input Friend file path] [input AccessLog file path] [output file path] [Using combiner, 1:0]");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "FTask - Task f");
		job.setJarByClass(FTask.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FirstReducer.class);
		if (args[3].equals("1")) {
			job.setCombinerClass(FirstCombiner.class);
			System.out.println("Combiner would be used.");
		} else {
			System.out.println("Combiner would not be used.");
		}
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, FriendsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, AccessLogMapper.class);
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		FileOutputFormat.setOutputPath(job, new Path(args[2] + "/" + timeStamp));
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

		public int getWhatPage() {
			return whatPage;
		}
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

		public int getPersonId() {
			return personId;
		}

		public int getMyFriend() {
			return myFriend;
		}
	}
}
