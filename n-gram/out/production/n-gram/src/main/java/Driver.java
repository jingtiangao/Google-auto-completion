package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Driver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		//job1
		System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop-2.7.2\\hadoop-2.7.2");
		Configuration conf1 = new Configuration();
		conf1.set("textinputformat.record.delimiter", ".");

		conf1.set("noGram", args[2]);

		Job job1 = Job.getInstance();
		/* job1.setJobName("NGram"); */
		job1.setJarByClass(Driver.class);

		job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
		job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));

//		job1.waitForCompletion(true);
		
		//how to connect two jobs?
		// last output is second input
		
		//2nd job
		Configuration conf2 = new Configuration();
		conf2.set("threashold", args[3]);
		conf2.set("n", args[4]);

		DBConfiguration.configureDB(conf2, 
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://localhost:3306/test",
				"root",
				"root");

		//Job job2 = Job.getInstance(conf2);
		addTmpJar("D:\\Sparkpro\\Project2\\Project2~\\Project2-1\\lib", conf2);
		Job job2 =new Job(conf2);
		job2.addArchiveToClassPath(new Path("file:///D:\\Sparkpro\\Project2\\Project2~\\Project2-1\\lib\\mysql-jdbc-5.1.25.jar"));
		job2.setJobName("Model");
		job2.setJarByClass(Driver.class);
		//DistributedCache.addArchiveToClassPath(new Path("lib/"),conf2);
		//jon2.addArchiveToClassPath(new Path("path_to_ur_connector"));
		//job2.getConfiguration().set("tmpjars", "file://D:\\Sparkpro\\Project2\\Project2~\\Project2-1\\lib\\mysql-jdbc-5.1.25.jar");

		//mapper与reducer output key v 不一样，要重新设置MapOutput key values

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(DBOutputWritable.class);
		job2.setOutputValueClass(NullWritable.class);
		
		job2.setMapperClass(LanguageModel.Map.class);
		job2.setReducerClass(LanguageModel.Reduce.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(DBOutputFormat.class);
		
		DBOutputFormat.setOutput(job2, "output", 
				new String[] {"starting_phrase", "following_word", "count"});

		TextInputFormat.setInputPaths(job2, args[1]);
			//job2.waitForCompletion(true);
	}
	public static void addTmpJar(String jarPath, Configuration conf) throws IOException {
		System.setProperty("path.separator", ":");
		FileSystem fs = FileSystem.getLocal(conf);
		String newJarPath = new Path(jarPath).makeQualified(fs).toString();
		String tmpjars = conf.get("tmpjars");
		if (tmpjars == null || tmpjars.length() == 0) {
			conf.set("tmpjars", newJarPath);
		} else {
			conf.set("tmpjars", tmpjars + "," + newJarPath);
		}
	}

}
