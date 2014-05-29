package tutorial;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

    static enum Counters { INPUT_WORDS }
    
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	
    private boolean caseSensitive = true;
    private Set<String> patternsToSkip = new HashSet<String>();

    private long numRecords = 0;
    private String inputFile;
    
    private void parseSkipFile(FSDataInputStream fsdis) throws IOException {
        String pattern = null;
        while ((pattern = fsdis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
    }
    
    public void setup(Context ctx) {
    	Configuration conf = ctx.getConfiguration();

    	caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
		inputFile = conf.get("map.input.file");
		if (conf.getBoolean("wordcount.skip.patterns", false)) {
			try {
				FileSystem fs = FileSystem.get(conf);
				URI[] patternsFiles = ctx.getCacheFiles();
				for (URI patternsFile : patternsFiles) {
					Path filePath = new Path(patternsFile);
					if(fs.exists(filePath)){
						parseSkipFile(fs.open(filePath));
					}
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			}

		}
	}
    
	@Override
	public void map(Object key, Text value, Context contex) throws IOException, InterruptedException {
		String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
        for (String pattern : patternsToSkip) {
          line = line.replaceAll(pattern, "");
        }
		
		// Break line into words for processing
		StringTokenizer wordList = new StringTokenizer(line);
		while (wordList.hasMoreTokens()) {
			word.set(wordList.nextToken());
			contex.write(word, one);
	        //contex.getCounter(Counters.INPUT_WORDS).increment(1);

		}

		//magic number? 100 is probably the default map size, if so get/set it from conf
        if ((++numRecords % 100) == 0) { 
        	contex.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
        }
		
	}
	
}