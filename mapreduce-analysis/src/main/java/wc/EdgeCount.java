package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;


public class EdgeCount extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(EdgeCount.class);
	private static final String COUNTER_GROUP = "CounterGroup";
	private static final String EDGE_TWO_PATH_CARDINALITY_COUNT = "EdgeTwoPathCardinalityCount";

	public static class EdgeCountMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text outvalue = new Text();
		private static long MAX = 500;

		@Override
		protected void setup(Context context) throws IOException {
			MAX = Long.parseLong(context.getConfiguration().get("max.value"));
		}

		@Override
		public void map(Object key, Text value, Context context) {
			String[] tokens = value.toString().split(",");
			if (tokens.length >= 2 && !tokens[0].isEmpty() && !tokens[1].isEmpty()) {
				String src = tokens[0];
				String dest = tokens[1];
				if (Integer.parseInt(src) >= MAX || Integer.parseInt(dest) >= MAX) {
					return;
				}
				Counter counter = context.getCounter(EdgeCount.COUNTER_GROUP, EdgeCount.EDGE_TWO_PATH_CARDINALITY_COUNT);
				counter.increment(1);
			}
		}
	}

	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "EDGE COUNT");
		job.setJarByClass(EdgeCount.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		String maxvalue = args[2];
		job.getConfiguration().set("max.value", maxvalue);
		job.setMapperClass(EdgeCountMapper.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Add the user files to the DistributedCache
		FileInputFormat.setInputDirRecursive(job, true);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		if (job.waitForCompletion(true)) {
			Counter c = job.getCounters().findCounter(EdgeCount.COUNTER_GROUP, EdgeCount.EDGE_TWO_PATH_CARDINALITY_COUNT);
			logger.info(EDGE_TWO_PATH_CARDINALITY_COUNT + " = " + c.getValue());
			return 0;
		}
		return 1;
	}

	public static void main(final String[] args) {

		try {
			ToolRunner.run(new EdgeCount(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}
