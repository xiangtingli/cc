import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.Accumulator;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.Comparator;
import java.util.Collection;
import java.io.Serializable;

public final class PageRank
{
	//static final String _arcs = "s3n://s15-p42-part2/wikipedia_arcs";	
	//static final String _mapping = "s3n://s15-p42-part2/wikipedia_mapping";
	static final int ITER_NUM = 10;
	
	public static void main(String[] args) throws Exception
	{
		SparkConf cnf = new SparkConf().setAppName("PageRank");
		JavaSparkContext ctx = new JavaSparkContext(cnf);
		//JavaRDD<String> arcs = ctx.textFile(_arcs);
		//JavaRDD<String> mapping = ctx.textFile(_mapping);

		final String _arcs = args[0];
		final String _mapping = args[1];

		JavaRDD<String> arcs = ctx.textFile(_arcs);
		JavaRDD<String> mapping = ctx.textFile(_mapping);
		
		// construct graph : <non-dangling page : {outbound links}>
		JavaPairRDD<String, Iterable<String> > links0 = arcs.mapToPair(new PairFunction<String, String, String>() {
			@Override
      		public Tuple2<String, String> call(String s)
      		{
        		String[] ss = s.split("\t");
        		return new Tuple2<String, String>(ss[0], ss[1]);
      		}
    	}).distinct().groupByKey();

		// find dangling pages
		JavaPairRDD<String, Iterable<String> > danglings0 = mapping.mapToPair(new PairFunction<String, String, Iterable<String> >() {
			@Override
      		public Tuple2<String, Iterable<String> > call(String s)
      		{
        		String[] ss = s.split("\t");
        		return new Tuple2<String, Iterable<String> >(ss[0], new ArrayList<String>());
      		}
    	});
    	
    	long numNodes = danglings0.count();
    	final Broadcast<Long> numDocs = ctx.broadcast(numNodes);
    	    	
    	// complete and init graph 
    	// links - <page : {outbound links}>
    	// ranks - <page : rank>
    	JavaPairRDD<String, Iterable<String> > links = links0.union(danglings0.subtractByKey(links0)).cache();
    	
		JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
		  	@Override
		  	public Double call(Iterable<String> rs)
		  	{
		    	return 1.0;
		  	}
		});	
    	
		// interatively calculate page_rank
		for (int i = 0; i < ITER_NUM; ++i)
		{
			// calculate dangling compensation
			final Accumulator<Double> accum = ctx.accumulator(0.0);
			ranks.subtractByKey(links0).values().foreach(new VoidFunction<Double>() {
				@Override
				public void call(Double val)
				{
				    accum.add(val);
				}
			});
			final Broadcast<Double> accVal = ctx.broadcast(accum.value());
			
			// calculates page contributions to the rank of other pages (dangling pages return empty list)
	 	  	JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
				@Override
				public Iterable<Tuple2<String, Double> > call(Tuple2<Iterable<String>, Double> t)
				{
				    int neibNum = 0;
				    if(t._1 instanceof Collection<?>)
			  		{
			  			neibNum = ((Collection<?>)t._1).size();
			  		}
				    List<Tuple2<String, Double> > ret = new ArrayList<Tuple2<String, Double> >();
				    for (String s : t._1)
				    {
				    	ret.add(new Tuple2<String, Double>(s, t._2() / neibNum));
				    }
				    return ret;
				}
			  });

			// re-calculate page_rank based on neighbor contributions
			ranks = contribs.reduceByKey(new Function2<Double, Double, Double>() {
				@Override
				public Double call(Double a, Double b)
				{
			  		return a + b;
				}
			}).mapValues(new Function<Double, Double>() {
				@Override
				public Double call(Double sum)
				{
					return 0.15 + (sum + accVal.value() / numDocs.value()) * 0.85;
				}
			});
		}
		
		// take top 100 ranked pages
		
		JavaPairRDD<String, String> titles = mapping.mapToPair(new PairFunction<String, String, String>() {
			@Override
      		public Tuple2<String, String> call(String s)
      		{
        		String[] ss = s.split("\t");
        		return new Tuple2<String, String>(ss[0], ss[1]);
      		}
    	});
    	
    	JavaPairRDD<String, Double> new_ranks = ranks.join(titles).mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, String> >, String, Double>() {
			@Override
      		public Tuple2<String, Double > call(Tuple2<String, Tuple2<Double, String> > t)
      		{
        		return new Tuple2<String, Double>(t._2._2, t._2._1);
      		}
    	});
    	
		JavaPairRDD<String, Double> res = ctx.parallelizePairs(new_ranks.takeOrdered(100, TupleComparator.Instance));
		
		res.saveAsTextFile(args[2]);	
			
		ctx.stop();
  	}
  	
  	static class TupleComparator implements Comparator<Tuple2<String, Double> >, Serializable
  	{
		final static TupleComparator Instance = new TupleComparator();
	   	public int compare(Tuple2<String, Double> t1, Tuple2<String, Double> t2)
	   	{
			return -t1._2.compareTo(t2._2);
	   	}
	}
}
