package proj02.task02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

public class Main {

	public static class CustomFileInputFormat extends FileInputFormat<LongWritable, Text> {
		@Override
		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context) throws IOException,
				InterruptedException {
			return new CustomLineRecordReader();
		}
	}

	public static class CustomLineRecordReader extends RecordReader<LongWritable, Text> {
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		private int maxLineLength;
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		@Override
		public void initialize(InputSplit genericSplit,
							   TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			boolean skipFirstLine = false;
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}

			//In most part of this file, we just use
			//the original implementation code from Hadoop.
			//And we made some modification here.
			//By default, LineReader would split the record line by line,
			//here we would specify the "}" as delimiter.
			in = new LineReader(fileIn, job, "}".getBytes());

			if (skipFirstLine) {
				Text dummy = new Text();
				start += in.readLine(dummy, 0,
						(int) Math.min(
								(long) Integer.MAX_VALUE,
								end - start));
			}
			this.pos = start;
		}

		@Override
		public boolean nextKeyValue() throws IOException {
			if (key == null) {
				key = new LongWritable();
			}
			key.set(pos);

			if (value == null) {
				value = new Text();
			}
			int newSize = 0;
			while (pos < end) {
				newSize = in.readLine(value, maxLineLength,
						Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
				if (newSize == 0) {
					break;
				}
				pos += newSize;
				if (newSize < maxLineLength) {
					break;
				}
			}
			if (newSize == 0 || !value.toString().contains("{")) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			//We would do some modification to the return value here.
			StringBuilder stringBuilder = new StringBuilder();
			String temp[] = value.toString().split(System.getProperty("line.separator"));
			for (String str : temp) {
				if (str.contains(":")) {
					stringBuilder.append(str.replace("\"", "").replace("\t","").replace(": ", ":"));
				}
			}
			return new Text(stringBuilder.toString());
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		@Override
		public void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
	}

	public static class FirstMapper extends Mapper<Object, Text, LongWritable, LongWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			LongWritable elevation = new LongWritable();
			LongWritable one = new LongWritable(1);
			String stringSplits[] = value.toString().split(",");
			for (String str : stringSplits) {
				if (str.contains("Elevation") && str.contains(":")) {
					elevation.set(Integer.parseInt(str.split(":")[1]));
					context.write(elevation, one);
					break;
				}
			}
		}
	}

	public static class FirstReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (LongWritable val : values) {
				count += val.get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input file path] [output file path]");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task 02");
		job.setInputFormatClass(CustomFileInputFormat.class);
		job.setJarByClass(Main.class);
		job.setMapperClass(FirstMapper.class);
		job.setCombinerClass(FirstReducer.class);
		job.setReducerClass(FirstReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		CustomFileInputFormat.addInputPath(job, new Path(args[0]));
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/"+ timeStamp));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
