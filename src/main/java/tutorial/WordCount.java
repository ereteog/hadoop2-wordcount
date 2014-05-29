package tutorial;

import java.util.*;
	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
	
	public class WordCount extends Configured implements Tool {
	
	    public int run(String[] args) throws Exception {

	        if (args.length < 2) {
	          System.out.println("usage: [input] [output]");
	          System.exit(-1);
	        }

			List<String> other_args = new ArrayList<String>();
			
			//Get the conf preconfigured from command line
			//(params with -D key=value are automatically added to conf)
			Configuration conf = getConf();
			
	        Job job = Job.getInstance(getConf());
			

	        //PARSE other arguments (here we have a custom -skip param)
			for (int i=0; i < args.length; ++i) {
				if ("-skip".equals(args[i])) {
					//add file to distributed cache
					job.addCacheFile(new Path(args[++i]).toUri());
					//add wordcount.skip.patterns param to conf
					job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			    } else {
			      other_args.add(args[i]);
			    }
			}
	        
	        job.setJobName("wordcount");
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	 
	        job.setMapperClass(WordMapper.class); 
	        job.setReducerClass(SumReducer.class);  
	 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	 
	        FileInputFormat.setInputPaths(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 
	        job.setJarByClass(WordCount.class);
	        job.submit();
	        return 0;
	    }
	
	    public static void main(String[] args) throws Exception {
	      int res = ToolRunner.run(new Configuration(), new WordCount(), args);
	      System.exit(res);
	    }
	}