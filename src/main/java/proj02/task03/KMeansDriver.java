package proj02.task03;//package KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class KMeansDriver {
	public static void main(String[] args) throws Exception {
		int repeated = 0;
		boolean converged = false;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 8) {
			System.err.println("Usage: <using Combiner> <in> <out> <oldcenters> <newcenters> <k> <threshold> <iterations>");
			for (int i = 0; i < otherArgs.length; i++) {
				System.err.println(Integer.toString(i) + otherArgs[i]);
			}
			System.exit(2);
		}
		conf.set("old.center.path", otherArgs[3]);
		conf.set("new.center.path", otherArgs[4]);
		conf.set("K", otherArgs[5]);
		conf.set("threshold", otherArgs[6]);
		conf.set("R", otherArgs[7]);
		conf.set("include.center.in.mapper", "0");

		// Run the map reduce job recursively,
		// util we reached the repeat limitation or the distance converged
		do {
			++repeated;
			conf.set("repeated", String.valueOf(repeated));

			Job job = new Job(conf, "KMeansCluster");
			job.setJarByClass(KMeansDriver.class);

			Path in = new Path(otherArgs[1]);
			Path out = new Path(otherArgs[2]);
			FileInputFormat.addInputPath(job, in);
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(out)) {
				fs.delete(out, true);
			}
			FileOutputFormat.setOutputPath(job, out);

			job.setMapperClass(KMeansMapper.class);
			if (otherArgs[0].equals("1")) {
				job.setCombinerClass(KMeansCombiner.class);
			}
			job.setReducerClass(KMeansReducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);
			System.out.println("****************************************");
			System.out.println("We have repeated " + repeated + " times.");
			if (JobIterator.isConverged(conf)) {
				System.out.println("Calculation has converged.");
				break;
			} else if (repeated >= Integer.parseInt(otherArgs[7])) {
				System.out.println("Reached limitation of repeating.");
				break;
			} else {
				JobIterator.prepaerNextIteration(otherArgs[3], otherArgs[4]);
				System.out.println("****************************************");
			}
		} while (true);
		System.out.println("****************************************");
		cluster(args, converged);
	}

	public static void cluster(String[] args, boolean converged)
			throws IOException, InterruptedException, ClassNotFoundException {
		//Use mapper and center file to output the clustering information
		System.out.println("Using mapper to output clustering information...");
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("old.center.path", otherArgs[3]);
		conf.set("K", otherArgs[5]);
		conf.set("include.center.in.mapper", "1");
		conf.set("converged", converged ? "1" : "0");
		Job job = new Job(conf, "KMeansCluster");
		job.setJarByClass(KMeansDriver.class);

		Path in = new Path(otherArgs[1]);
		Path out = new Path(otherArgs[2] + "/" + "clustering_info");
		FileInputFormat.addInputPath(job, in);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(job, out);

		//No reducer needed here.
		job.setMapperClass(KMeansMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
	}
}
