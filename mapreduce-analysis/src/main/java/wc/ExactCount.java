package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ExactCount extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ExactCount.class);
	private static final String COUNTER_GROUP = "CounterGroup";
	private static final String TOTAL_EXACT_TRIANGLES = "TotalExactTriangles";

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, TextIntPair> {
		private final static IntWritable one = new IntWritable(1);
		private final Text outgoing = new Text("outgoing");
		private final Text incoming = new Text("incoming");

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length >= 2 && !tokens[0].isEmpty() && !tokens[1].isEmpty()) {
				final IntWritable src = new IntWritable(Integer.parseInt(tokens[0]));
				final IntWritable dest = new IntWritable(Integer.parseInt(tokens[1]));
				final TextIntPair incomingPair = new TextIntPair();
				incomingPair.set(incoming, one);
				final TextIntPair outgoingPair = new TextIntPair();
				outgoingPair.set(outgoing, one);
				context.write(src, incomingPair);
				context.write(dest, outgoingPair);
			}
		}
	}

	public static class IntSumReducer extends Reducer<IntWritable, TextIntPair, IntWritable, Text> {
		private final Text result = new Text();

		@Override
		public void reduce(final IntWritable key, final Iterable<TextIntPair> values, final Context context)
				throws IOException, InterruptedException {
			long incomingCount = 0;
			long outgoingCount = 0;
			for (final TextIntPair val : values) {
				Text direction = val.getFirst();
				IntWritable count = val.getSecond();

				if (direction.toString().equals("incoming")) {
					incomingCount += count.get();
				} else {
					outgoingCount += count.get();
				}
			}
			Counter counter = context.getCounter(ExactCount.COUNTER_GROUP, ExactCount.TOTAL_EXACT_TRIANGLES);
			counter.increment(incomingCount * outgoingCount);
			long exactTriangles = incomingCount * outgoingCount;
			result.set(Long.toString(exactTriangles));
			context.write(key, result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "EXACT TRIANGLES");
		job.setJarByClass(ExactCount.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TextIntPair.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (job.waitForCompletion(true)) {
			// Retrieve and print the counter value
			Counter c = job.getCounters().findCounter(ExactCount.COUNTER_GROUP, ExactCount.TOTAL_EXACT_TRIANGLES);
			System.out.println(TOTAL_EXACT_TRIANGLES + " = " + c.getValue());
			return 0;
		}
		return 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new ExactCount(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}