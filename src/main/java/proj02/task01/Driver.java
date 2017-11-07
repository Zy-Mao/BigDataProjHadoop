package proj02.task01;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * The entry point for the WordCount example,
 * which setup the Hadoop job with Map and Reduce Class
 *
 * @author Raman
 */
public class Driver {

	public static class MapClass extends Mapper<Object, Text, LongWritable, Text> {
		private RECT allrt = new RECT();


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Mapperwetr");
			String line = value.toString();
			String[] point = line.split(",");
			int pointx = Integer.parseInt(point[0]);
			int pointy = Integer.parseInt(point[1]);
			System.out.println("before loop");
			for (int x = 1; x <= allrt.lenx1(); x = x + 1) {

				int rectX1 = Integer.parseInt(allrt.getX1(x));
				int rectY1 = Integer.parseInt(allrt.getY1(x));
				int rectX2 = Integer.parseInt(allrt.getX2(x));
				int rectY2 = Integer.parseInt(allrt.getY2(x));
				String reg = allrt.getR(x);

				String outputString1 = ("\n<" + "r" + reg + "(" + String.valueOf(pointx) + "," + String.valueOf(pointy) + ")" + ">");


				if (rectX1 <= pointx & rectY1 <= pointy & rectX2 >= pointx & rectY2 >= pointy) {
					context.write((LongWritable) key, new Text(outputString1));

				}
//				if (rectX1 <= pointx) {
//					//context.write(key, new Text(outputString));
//				}
//
//				while (st.hasMoreTokens()) {
//					String wordText = st.nextToken();
//
//					if (!stopWords.contains(wordText.toLowerCase())) {
//						word.set(wordText);
//						context.write(word, one);
//					}
//				}
			}
		}

//		private final static IntWritable one = new IntWritable(1);
//		private Text region = new Text();
//		private Set stopWords = new HashSet();

		@Override
		protected void setup(Context Text) throws IOException, InterruptedException {
			System.out.println("setup");
			try {
				//Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				Path[] rectFiles = DistributedCache.getLocalCacheFiles(Text.getConfiguration());
				if (rectFiles != null && rectFiles.length > 0) {
					for (Path rects : rectFiles) {
						readFile(rects);
					}
				}
			} catch (IOException ex) {
				System.err.println("Exception in mapper setup: " + ex.getMessage());
			}
		}

		/**
		 * map function of Mapper parent class takes a line of text at a time
		 * splits to tokens and passes to the context as word along with value as one
		 */


		public static class RECT {
			private ArrayList<String> rectR = new ArrayList<String>();
			private ArrayList<String> rectX1 = new ArrayList<String>();
			private ArrayList<String> rectY1 = new ArrayList<String>();
			private ArrayList<String> rectX2 = new ArrayList<String>();
			private ArrayList<String> rectY2 = new ArrayList<String>();


			public void setX1(String rectX1) {
				System.out.println("recta");
				this.rectX1.add(rectX1);
			}

			public void setR(String rectR) {
				this.rectR.add(rectR);
			}

			public void setY1(String rectY1) {
				this.rectY1.add(rectY1);
			}

			public void setX2(String rectX2) {
				this.rectX2.add(rectX2);
			}

			public void setY2(String rectY2) {
				this.rectY2.add(rectY2);
			}

			public int lenx1() {
				return rectX1.size();
			}

			public int leny2() {
				return rectY2.size();
			}

			public String getX1(int index) {
				return rectX1.get(index);
			}

			public String getY1(int index) {
				return rectY1.get(index);
			}

			public String getX2(int index) {
				return rectX2.get(index);
			}

			public String getY2(int index) {
				return rectY2.get(index);
			}

			public String getR(int index) {

				return rectR.get(index);
			}

		}

		private void readFile(Path filePath) {
			System.out.println("readfile");
			try {
				//BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
				BufferedReader rects = new BufferedReader(new FileReader(filePath.toString()));
				//String stopWord = null;
				String linerect = "";
				while ((linerect = rects.readLine()) != null) {
					String[] rect = linerect.split(",");
					allrt.setR(rect[0]);
					allrt.setX1(rect[1]);
					allrt.setY1(rect[2]);
					allrt.setX2(rect[3]);
					allrt.setY2(rect[4]);
				}
			} catch (IOException ex) {
				System.err.println("Exception while reading recta file: " + ex.getMessage());
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Usage: Facebook Users <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(Driver.class);
		job.setJobName("Find Facebook User");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		DistributedCache.addCacheFile(new Path(args[1]).toUri(), job.getConfiguration());
		job.setMapperClass(MapClass.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
//		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
	
	
	
