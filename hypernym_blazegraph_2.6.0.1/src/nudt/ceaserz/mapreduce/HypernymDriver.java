package nudt.ceaserz.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HypernymDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "hypernym_1.1");
		job.setJarByClass(nudt.ceaserz.mapreduce.HypernymDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(nudt.ceaserz.mapreduce.HypernymMap.class);
		// TODO: specify a reducer
		job.setReducerClass(nudt.ceaserz.mapreduce.HypernymReduce.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/user/ceaserz/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/ceaserz/output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
