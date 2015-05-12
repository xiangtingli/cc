import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayDeque;
import java.util.Iterator;

public class ngram
{
	public static class Map extends Mapper<Object, Text, Text, IntWritable>
  	{
		private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	private final int MAX_N = 5;
		ArrayDeque<String> m_queue = new ArrayDeque<String>();
		private int count = 0;
		
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    	{
    		String line = value.toString();
    		line = line.replaceAll("[^a-zA-Z ]", " ").toLowerCase();
    		StringTokenizer token = new StringTokenizer(line);
      		while (token.hasMoreTokens())
      		{
      			if(count == MAX_N)
      			{
      				StringBuilder str = new StringBuilder(m_queue.poll());
      				--count;
      				word.set(str.toString());
        			context.write(word, one);
      				for(Iterator itr = m_queue.iterator(); itr.hasNext(); )
      				{
      					str.append(" " + itr.next());
      					word.set(str.toString());
        				context.write(word, one);
      				}      				
      			}
      			m_queue.add(token.nextToken());
      			++count;
      		}
      		while(count > 0)
  			{
  				StringBuilder str = new StringBuilder(m_queue.poll());
  				--count;
  				word.set(str.toString());
    			context.write(word, one);
  				for(Iterator itr = m_queue.iterator(); itr.hasNext(); )
  				{
  					str.append(" " + itr.next());
  					word.set(str.toString());
    				context.write(word, one);
  				}      				
  			}
    	}
  	}

  	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
  	{
  		private IntWritable result = new IntWritable();
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	    {
	    	int sum = 0;
      		for (IntWritable val : values)
      		{
        		sum += val.get();
      		}
      		result.set(sum);
      		context.write(key, result);
    	}
  	}

  	public static void main(String[] args) throws Exception
  	{
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "n-gram");
    	job.setJarByClass(ngram.class);
    	job.setMapperClass(Map.class);
    	job.setCombinerClass(Reduce.class);
    	job.setReducerClass(Reduce.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
