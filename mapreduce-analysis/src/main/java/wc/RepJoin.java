package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;


public class RepJoin extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(RepJoin.class);
	private static final String COUNTER_GROUP = "CounterGroup";
	private static final String TOTAL_APPROX_TRIANGLES = "TotalApproxTriangles";

	public static class ReplicatedJoinTrianglesMapper extends
			Mapper<Object, Text, Text, Text> {

		private HashMap<String, ArrayList<String>> sourceToDestinationMap = new HashMap<>();

		private Text outvalue = new Text();
		private static int MAX = 500;

		@Override
		protected void setup(Context context) throws IOException {
			MAX = Integer.parseInt(context.getConfiguration().get("max.value"));
			// Retrieve the cached files
			URI[] cacheFiles = context.getCacheFiles();
			logger.info("cacheFiles: " + cacheFiles[0].getPath().toString());
			if (cacheFiles == null || cacheFiles.length == 0) {
				throw new RuntimeException("User information is not set in DistributedCache");
			} else {
				logger.info("cacheFiles: " + cacheFiles[0].getPath().toString());
			}

			if (cacheFiles != null && cacheFiles.length > 0) {
				String str;
				// remove this comment when running on EMR /**
				FileSystem fs = FileSystem.get(URI.create("s3://cs6240-demo-bucket-harivp/"), context.getConfiguration());
				fs.setWorkingDirectory(new Path("/input/"));
				FSDataInputStream in = fs.open(new Path("edges.csv"));
				logger.info("in: " + in.toString());
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				// **/
				// remove this comment when running on local
//				BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].getPath().toString()));
				while ((str = reader.readLine()) != null) {
					String[] tokens = str.split(",");
					if (tokens.length >= 2 && !tokens[0].isEmpty() && !tokens[1].isEmpty()) {
						String src = tokens[0];
						String dest = tokens[1];
						if (Integer.parseInt(src) >= MAX || Integer.parseInt(dest) >= MAX) {
							continue;
						}
						sourceToDestinationMap.computeIfAbsent(src, k -> new ArrayList<>()).add(dest);
					}
				}
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length >= 2 && !tokens[0].isEmpty() && !tokens[1].isEmpty()) {
				String src = tokens[0];
				String dest = tokens[1];
				if (Integer.parseInt(src) >= MAX || Integer.parseInt(dest) >= MAX) {
					return;
				}
				Counter counter = context.getCounter(RepJoin.COUNTER_GROUP, RepJoin.TOTAL_APPROX_TRIANGLES);
						for (String destination : sourceToDestinationMap.getOrDefault(dest, new ArrayList<>())) {
							for( String destination2 : sourceToDestinationMap.getOrDefault(destination, new ArrayList<>())) {
								if (src.equals(destination2)) {
									outvalue.set(value.toString() + "," + destination + "," + destination2);
									context.write(new Text(src), outvalue);
									counter.increment(1);
								}
							}
						}
			}
		}
	}

	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "EXACT TRIANGLES");
		job.setJarByClass(RepJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		String maxvalue = args[2];
		job.getConfiguration().set("max.value", maxvalue);
		job.setMapperClass(ReplicatedJoinTrianglesMapper.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Add the user files to the DistributedCache
		FileInputFormat.setInputDirRecursive(job, true);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.addCacheFile(new Path(args[0]).toUri());

		if (job.waitForCompletion(true)) {
			Counter c = job.getCounters().findCounter(RepJoin.COUNTER_GROUP, RepJoin.TOTAL_APPROX_TRIANGLES);
			logger.info(TOTAL_APPROX_TRIANGLES + " = " + (c.getValue())/3);
			return 0;
		}
		return 1;
	}

	public static void main(final String[] args) {

		try {
			ToolRunner.run(new RepJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}
