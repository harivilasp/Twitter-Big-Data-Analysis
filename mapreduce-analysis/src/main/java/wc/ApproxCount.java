package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.ArrayList;

public class ApproxCount extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ApproxCount.class);
	private static final String COUNTER_GROUP = "CounterGroup";
	private static final String TWO_PATH_CARDINALITY = "TwoPathCardinality";
	private static int MAX = 250;



	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text outvalue1 = new Text();
		private Text outvalue2 = new Text();

		@Override
		public void setup(Context context) {

			MAX = Integer.parseInt(context.getConfiguration().get("max.value"));
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			if (tokens.length >= 2 && !tokens[0].isEmpty() && !tokens[1].isEmpty()) {
				String src = tokens[0];
				String dest = tokens[1];
				if (Integer.parseInt(src) >= MAX || Integer.parseInt(dest) >= MAX) {
					return;
				}
				final Text source = new Text(src);
				final Text destination = new Text(dest);
				outvalue1.set("S" + value.toString());
				outvalue2.set("D" + value.toString());

				context.write(source, outvalue1);
				context.write(destination, outvalue2);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {


		private ArrayList<Text> listS = new ArrayList<Text>();
		private ArrayList<Text> listD = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Clear our lists
			listS.clear();
			listD.clear();

			for (Text t : values) {
				if (t.charAt(0) == 'S') {
					listS.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'D') {
					listD.add(new Text(t.toString().substring(1)));
				}
			}
			if(listS.isEmpty() || listD.isEmpty()) {
				return;
			}
			Counter counter = context.getCounter(ApproxCount.COUNTER_GROUP, ApproxCount.TWO_PATH_CARDINALITY);
			for (Text S : listS) {
				for (Text D : listD) {
					counter.increment(1);
					context.write(D, S);
				}
			}
		}

	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "EXACT TRIANGLES");
		job.setJarByClass(ApproxCount.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		String maxvalue = args[2];
		job.getConfiguration().set("max.value", maxvalue);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (job.waitForCompletion(true)) {
			// Retrieve and print the counter value
			Counter c = job.getCounters().findCounter(ApproxCount.COUNTER_GROUP, ApproxCount.TWO_PATH_CARDINALITY);
			System.out.println(TWO_PATH_CARDINALITY + " = " + c.getValue());
			return 0;
		}
		return 1;
	}

	public static void main(final String[] args) {
		try {
			ToolRunner.run(new ApproxCount(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}