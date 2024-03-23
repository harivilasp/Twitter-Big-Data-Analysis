package wc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RedJoin extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(RedJoin.class);
	private static int MAX = 500;
	private static final String COUNTER_GROUP = "CounterGroup";
	private static final String TOTAL_APPROX_TRIANGLES = "TotalApproxTriangles";

	public static class FirstMapper extends Mapper<Object, Text, Text, Text> {
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
	public static class FirstReducer extends Reducer<Text, Text, Text, Text> {

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

			for (Text S : listS) {
				for (Text D : listD) {
					context.write(D, S);
				}
			}
		}
	}
	public static class SecondMapper extends Mapper<Object, Text, Text, Text> {
		private Text outvalue = new Text();
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
				outvalue.set("S" + value.toString());
				context.write(source, outvalue);
			}
		}
	}

	public static class ThirdMapper extends Mapper<Object, Text, Text, Text> {
		private Text outvalue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length >= 2 && !tokens[0].isEmpty() && !tokens[1].isEmpty()) {
				String dest = tokens[1].split(",")[1];
				final Text destination = new Text(dest);
				outvalue.set("D" + value.toString());
				context.write(destination, outvalue);
			}
		}
	}


	public static class SecondReducer extends Reducer<Text, Text, Text, Text> {
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
			Counter counter = context.getCounter(RedJoin.COUNTER_GROUP, RedJoin.TOTAL_APPROX_TRIANGLES);

			for (Text S : listS) {
				for (Text D : listD) {
					String[] tokensS = S.toString().split(",");
					String[] tokensD = D.toString().split(",");
					if (tokensS[1].equals(tokensD[0])){
						context.write(S, D);
						counter.increment(1);
					}
				}
			}
		}
	}


	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "EXACT TRIANGLES");
		job.setJarByClass(RedJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		String maxvalue = args[2];
		job.getConfiguration().set("max.value", maxvalue);
		job.setMapperClass(FirstMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FirstReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (!job.waitForCompletion(true)) {
			return 1;
		}
		// Job 2
		final Configuration conf2 = getConf();
		final Job job2 = Job.getInstance(conf2, "Second join job");
		job2.setJarByClass(RedJoin.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path("s3://cs6240-demo-bucket-harivp/output2"));
//		FileOutputFormat.setOutputPath(job2, new Path("output2"));
		// Job 2 Mapper
		job2.setMapperClass(SecondMapper.class);
		job2.getConfiguration().set("max.value", maxvalue);
		MultipleInputs.addInputPath(job2, new Path(args[0]),
				TextInputFormat.class, SecondMapper.class);

		MultipleInputs.addInputPath(job2, new Path(args[1]),
				TextInputFormat.class, ThirdMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(SecondReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		if (job2.waitForCompletion(true)) {
			// Retrieve and print the counter value
			Counter c = job2.getCounters().findCounter(RedJoin.COUNTER_GROUP, RedJoin.TOTAL_APPROX_TRIANGLES);
			logger.info(TOTAL_APPROX_TRIANGLES + " = " + (c.getValue())/3);
			System.out.println(TOTAL_APPROX_TRIANGLES + " = " + (c.getValue())/3);
			return 0;
		}
		return 2;
	}

	public static void main(final String[] args) {
		try {
			ToolRunner.run(new RedJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}

