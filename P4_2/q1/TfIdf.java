import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Collection;

public final class TfIdf
{
	//private static final String inputFile = "s3://s15-p42-part1-easy/data/";
	//private static final String outputFile = "s3://s15-p42-part1/";
	
	public static void main(String[] args) throws Exception
	{
		SparkConf cnf = new SparkConf().setAppName("TF-IDF");
		JavaSparkContext ctx = new JavaSparkContext(cnf);
		//JavaRDD<String> data = ctx.textFile(inputFile);
		JavaRDD<String> data = ctx.textFile(args[0]);

		// preprocess files
		JavaRDD<String> prep = data.flatMap(new FlatMapFunction<String, String>(){
			@Override
		    public Iterable<String> call(String s)
			{
				// find title and text
		  		// not using .split("\t") since not sure if "\t" would appear in the article
				int pos1 = 0;
				int pos2 = s.indexOf('\t');		// skip page id
				if(pos2 < 0)
				{
					return null;
				}
				pos1 = pos2 + 1;
				pos2 = s.indexOf('\t', pos1);	// title
				if(pos2 < 0)
				{
					return null;
				}
				String title = s.substring(pos1, pos2);
				pos1 = pos2 + 1;
				pos2 = s.indexOf('\t', pos1);	// skip date
				if(pos2 < 0)
				{
					return null;
				}
				pos1 = pos2 + 1;
				pos2 = s.indexOf('\t', pos1);	// txt
				if(pos2 < 0)
				{
					return null;
				}
				String txt = s.substring(pos1);
		
				txt = txt.replaceAll("<[^>]*>", " ").replaceAll("\\\\n", " ").replaceAll("[^a-zA-Z ]", " ")
						 .replaceAll(" +", " ").trim().toLowerCase();
				String[] tt = txt.split(" ");
				for(int i = 0; i < tt.length; ++i)
				{
					StringBuilder sb = new StringBuilder(tt[i]);
					sb.append("@" + title);
					tt[i] = sb.toString();
				}
		    	return Arrays.asList(tt);	
			}
		});
		
		//prep.saveAsTextFile(args[1] + "/prep/"); 
		
		// calculate total doc number
		final double numTotalDocs = (double)prep.count();
		final Broadcast<Double> numDocs = ctx.broadcast(numTotalDocs);
				
		// caculate TF : < word,title : count >		
		JavaPairRDD<String, Integer> tf_interim = prep.mapToPair(new PairFunction<String, String, Integer>() {
  			@Override
  			public Tuple2<String, Integer> call(String s)
  			{
  				return new Tuple2<String, Integer>(s, 1);
  			}
		});
		
		JavaPairRDD<String, Integer> tf = tf_interim.reduceByKey(new Function2<Integer, Integer, Integer>() {
  			@Override
  			public Integer call(Integer cnt1, Integer cnt2)
  			{
  				return cnt1 + cnt2;
  			}
		});
						
		//tf.saveAsTextFile(args[1] + "/tf/"); 
		
		// caculate DF : <word,title : df>, in which all keys with same word have the same value
		JavaPairRDD<String, Iterable<String> > df_interim1 = prep.mapToPair(new PairFunction<String, String, String>() {
			@Override
		  	public Tuple2<String, String> call(String s)
		  	{
		  		int pos = s.indexOf('@');
		  		if(pos < 0)
		  		{
		  			return null;
		  		}
		    	return new Tuple2<String, String>(s.substring(0, pos), s.substring(pos + 1));
		  	}
		}).distinct().groupByKey();	
		
		//df_interim1.saveAsTextFile(args[1] + "/df_interim1/");
		
		JavaRDD<Tuple2<String, Integer> > df_interm2 = df_interim1.flatMap(new FlatMapFunction<Tuple2<String, Iterable<String> >, Tuple2<String, Integer> >() {
			@Override
		  	public Iterable<Tuple2<String, Integer> > call(Tuple2<String, Iterable<String> > t)
		  	{
		  		String word = t._1;
		  		int len = 0;
		  		if(t._2 instanceof Collection<?>)
		  		{
		  			len = ((Collection<?>)t._2).size();
		  		}
		  		ArrayList<Tuple2<String, Integer> > ret = new ArrayList<Tuple2<String, Integer> >();
		  		for(String s : t._2)
		  		{
		  			ret.add(new Tuple2<String, Integer>(word + "@" + s, len));
		  		}
		    	return ret;
		  	}
		});	
		
		JavaPairRDD<String, Integer> df = JavaPairRDD.fromJavaRDD(df_interm2);
				
		//df.saveAsTextFile(args[1] + "/df/"); 
				
		// calculate TF-IDF : <word,title : idf>		
		JavaPairRDD<String, Tuple2<Integer, Integer> > tf_df = tf.join(df);
		
		JavaPairRDD<String, Double> tf_idf = tf_df.mapValues(new Function<Tuple2<Integer, Integer>, Double>() {
			@Override
			public Double call(Tuple2<Integer, Integer> pair)
			{
				if(pair._2 == 0)
				{
					return 0.0;
				}
				double tfVal = (double)pair._1;
				double dfVal = (double)pair._2;
				return tfVal * Math.log(numDocs.value() / dfVal);
			}
		});
		 
		//tf_idf.saveAsTextFile(args[1] + "/res/"); 
		
		// filter and sort
		tf_idf.filter(new Function<Tuple2<String, Double>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, Double> t)
			{
				String s = (String)t._1;
				if(s == null || s.isEmpty())
				{
					return false;
				}
				int pos = s.indexOf('@');
		  		if(pos < 0)
		  		{
		  			return false;
		  		}
		  		s = s.substring(0, pos);
				return s.equals("cloud");
			}
		}).saveAsTextFile(args[1]);
		
		ctx.stop();
  	}
}
