import java.io.IOException;
import java.util.StringTokenizer;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.PriorityQueue; 
import java.util.Comparator;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

public class model
{
	// hbase parameters
	private static HTablePool m_htp;
	private static final String m_master = "ec2-52-5-237-146.compute-1.amazonaws.com";
	private static final String m_table = "tbl";
	private static final String m_colBase = "word";

	static class Intermediate implements WritableComparable<Intermediate> 
	{
		public String nxtWrd = ".";
		public int count = 0;
		
		public Intermediate() {}
		
		public Intermediate(int _n)
		{
			count = _n;
		}
				
		public Intermediate(String _w, int _n)
		{
			nxtWrd = _w;
			count = _n;
		}

		@Override
		public int compareTo(Intermediate other)
		{
			return nxtWrd == "." ? -1 : nxtWrd.compareTo(other.nxtWrd);
		}
		
		@Override
		public void write(DataOutput out) throws IOException
		{
        	out.writeUTF(nxtWrd);
        	out.writeInt(count);
        }
       
        @Override
       	public void readFields(DataInput in) throws IOException
       	{
        	nxtWrd = in.readUTF();
        	count = in.readInt();
       	}
       
        @Override
       	public int hashCode()
       	{
       		StringBuilder str = new StringBuilder(nxtWrd);
       		str.append(Integer.toString(count));
         	return str.hashCode();
       	}
	}

	public static class Map extends Mapper<Object, Text, Text, Intermediate>
  	{
    	private Text word = new Text();
    	private Intermediate res = new Intermediate();
    	
    	private Configuration conf;
		private int THRESH = 2;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			conf = context.getConfiguration();
		  	THRESH = conf.getInt("model.minTimes", 2);
		}
		
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    	{
    		String line = value.toString();
    		String[] ss = line.split("\t");
    		if(ss.length != 2)
    		{
    			return;
    		}
    		String phrase = ss[0];
    		int val = 0;
    		try
    		{
    			val = Integer.parseInt(ss[1]);
    		}
    		catch(NumberFormatException e)
    		{
    			return;
    		} 
    		if(val <= THRESH)
    		{
    			return;
    		}
    		
    		// write itself
    		word.set(phrase);
			res = new Intermediate(val);
			context.write(word, res);
    		
    		// write its pred, if any
    		int posSpc = phrase.lastIndexOf(' ');
    		if(posSpc > 0)
    		{
    			String tmp1 = phrase.substring(0, posSpc);
    			String tmp2 = phrase.substring(posSpc + 1);
    			word.set(tmp1);
    			res = new Intermediate(tmp2, val);
    			context.write(word, res);
    		}
    	}
  	}

  	public static class Reduce extends Reducer<Text, Intermediate, Text, Text>
  	{
  		private Text res = new Text();
  		private Configuration conf;
  		private int MAX_N = 5;
  		PriorityQueue<Intermediate> heap = new PriorityQueue();		
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			conf = context.getConfiguration();
		  	MAX_N = conf.getInt("model.maxNumber", 5);
		  	heap = new PriorityQueue(MAX_N, new Comparator<Intermediate>()
									  		{
									  			@Override
									  			public int compare(Intermediate lhs, Intermediate rhs)
												{
													if(lhs.count == rhs.count)
													{
														return lhs.nxtWrd.compareTo(rhs.nxtWrd);
													}
													return lhs.count > rhs.count ? -1 : 1;
												} 
									  		});
		}
		
	    public void reduce(Text key, Iterable<Intermediate> values, Context context) throws IOException, InterruptedException
	    {
	    	int base = 0;
      		for (Intermediate val : values)
      		{
      			if(val.nxtWrd.equals("."))
      			{
      				base = val.count;
      			}
      			else
      			{
      				heap.add(new Intermediate(val.nxtWrd, val.count));
      			}
      		}
      		if(heap.isEmpty())
      		{
      			return;
      		}
      		StringBuilder resStr = new StringBuilder();
      		int num = 0;
      		while(!heap.isEmpty() && num < MAX_N)
      		{
      			int scr = heap.peek().count;
      			double prob = (double)(scr) / (double)(base);
      			String tmp = heap.poll().nxtWrd;
      			resStr.append(tmp + "," + Double.toString(prob) + "\t");
      			++num;
      		}
      		resStr.deleteCharAt(resStr.length() - 1);
      		//PutData(key.toString(), res.toString());
      		
      		res.set(resStr.toString());
      		context.write(key, res);
    	}
  	}
  	
  	/*
  	private static void InitHBase() throws IOException, MasterNotRunningException, TableExistsException
  	{
  		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", m_master);
        m_htp = new HTablePool(config, 100);
        	
		
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor desc = new HTableDescriptor(m_table);
		// to be modified
		for(int i = 0; i < 5; ++i)
		{
			HColumnDescriptor tmp = new HColumnDescriptor((m_colBase + Integer.toString(i + 1)).getBytes());
			desc.addFamily(tmp);
		}
		admin.createTable(desc);
		
  	}
  	
  	private static void PutData(String rowKey, String vals)
	{
		HTableInterface htable = null;
		try
		{
			htable = m_htp.getTable(Bytes.toBytes(m_table));
			try
			{
				Put p = new Put(Bytes.toBytes(rowKey));
				String ss[] = vals.split("\t");
				for(int i = 0; i < ss.length; ++i)
				{
					p.add(Bytes.toBytes(m_colBase + Integer.toString(i + 1)), Bytes.toBytes(ss[i]), Bytes.toBytes("0"));
				}
				htable.put(p);
			}
			catch(Exception e0)
			{
				e0.printStackTrace();
			}
			finally
			{ 
				if (htable != null)
				{
					htable.close();
				}
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		catch(Exception e0)
		{
			e0.printStackTrace();
		}
		finally
		{
			try
			{
				if (htable != null)
				{
					htable.close();
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	*/

  	public static void main(String[] args) throws Exception
  	{
    	Configuration conf = new Configuration();
    	GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		String[] paths = new String[2];
		paths[0] = args[0];
		paths[1] = args[1];
		int MAX_N = 5;
		int THRESH = 2;
		if(remainingArgs.length % 2 != 0)
		{
			return;
		}
		for (int i = 0; i < remainingArgs.length; ++i)
		{
			if ("-n".equals(remainingArgs[i]))
			{
				try
				{
					MAX_N = Integer.parseInt(remainingArgs[++i]);
				}
				catch(NumberFormatException e)
				{
					e.printStackTrace();
				} 
		  	}
		  	else if ("-t".equals(remainingArgs[i]))
		  	{
		    	try
				{
					THRESH = Integer.parseInt(remainingArgs[++i]);
				}
				catch(NumberFormatException e)
				{
					e.printStackTrace();
				}
		  	}
		  	else if ("-i".equals(remainingArgs[i]))
		  	{
		  		paths[0] = remainingArgs[++i];
		  	}
		  	else if ("-o".equals(remainingArgs[i]))
		  	{
		  		paths[1] = remainingArgs[++i];
		  	}
		}
    	
    	//InitHBase();
    	
    	Job job = Job.getInstance(conf, "n-gram");
    	job.setJarByClass(model.class);
    	job.setMapperClass(Map.class);
    	//job.setCombinerClass(Reduce.class);
    	job.setReducerClass(Reduce.class);
    	job.addFileToClassPath(new Path("/lib/hbase-0.94.18.jar")); 
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	job.setMapOutputValueClass(Intermediate.class);
    	
    	job.getConfiguration().setInt("model.maxNumber", MAX_N);
    	job.getConfiguration().setInt("model.minTimes", THRESH);
    	FileInputFormat.addInputPath(job, new Path(paths[0]));
    	FileOutputFormat.setOutputPath(job, new Path(paths[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
