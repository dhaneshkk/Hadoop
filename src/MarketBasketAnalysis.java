
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Market Basket Analysis Algorithm: find the association rule for the list of items 
 * in a basket; That is, there are transaction data in a store
 * <ul>
 * <li>trx1: apple, cracker, soda, corn </li>
 * <l1>trax2: icecream, soda, bread</li>
 * <li>...</li>
 * <ul>
 * 
 * <p>
 * The code reads the data as 
 * key: first item
 * value: the rest of the items
 * 
 * <p>
 * And, count the possible associations as requested by user: two pairs, triples, ...
 * ( based on see <a href="http://dal-cloudcomputing.blogspot.com">Jongwook's Map/Reduce blog</a>)
 * 
 * @date: 03/28/2013
 * @author Dhanesh
 * @version: 1.0
 *
 */
public class MarketBasketAnalysis extends Configured implements Tool {
	
		
		/**
	 * MyMapper
	 * input: (key1, value1)
	 * output: <(key2, value2)>
	 * @author Dhanesh
	 *
	 */
	protected static class MarketBasketMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
		
		// Code is from http://stackoverflow.com/questions/11120985/guava-collections-limit-permutation-size by Mrswadge
		  public List<List<T>> processSubsets( List<T> set, int k ) {
		    if ( k > set.size() ) {
		      k = set.size();
		    }
		    List<List<T>> result = Lists.newArrayList();
		    List<T> subset = Lists.newArrayListWithCapacity( k );
		    for ( int i = 0; i < k; i++ ) {
		      subset.add( null );
		    }
		    return processLargerSubsets( result, set, subset, 0, 0 );
		  }

		  private List<List<T>> processLargerSubsets( List<List<T>> result, List<T> set, List<T> subset, int subsetSize, int nextIndex ) {
		    if ( subsetSize == subset.size() ) {
		      result.add( ImmutableList.copyOf( subset ) );
		    } else {
		      for ( int j = nextIndex; j < set.size(); j++ ) {
		        subset.set( subsetSize, set.get( j ) );
		        processLargerSubsets( result, set, subset, subsetSize + 1, j + 1 );
		      }
		    }
		    return result;
		  }

		//output key2: list of items paired; can be 2 or 3 ...
		private static final Text itemList = new Text();
		//output value2: number of the paired items in the item list
		private static final IntWritable countItemList = new IntWritable(1);
		
		/**
		 * 
		 * build <key, value> by sorting the input list
		 * If not sort the input, it may have duplicated list but not considered as a same list.
		 * ex: (a, b, c) and (a, c, b) might become different items to be counted if not sorted
		 * @param no_pairs	number of pairs associated
		 * @param itr		input line
		 * @param context	Hadoop Job context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void buildOutputWithSort(int no_pairs, StringTokenizer itr, Context context) throws IOException, InterruptedException{
			//key is collected with an empty item list
			List<String> itemsST = new LinkedList<String>();			
			while (itr.hasMoreTokens()){ 
				itemsST.add((String)itr.nextToken());			
			}
			//sort items not to duplicate the items
			// ex: (a, b, c) and (a, c, b) might become different items to be counted if not sorted
			Collections.sort(itemsST);
			PermutationsOfN<String> g = new PermutationsOfN<String>();
			Iterator it = itemsST.iterator();
			List<String> items = new LinkedList<String>();	
			while (it.hasNext()){ 
				items.add((String)it.next());			
				// add the item list of the key
				List<List<String>> outItems = null;
				String itemPair = null;
				for(int i=0;i<items.size();i++){ 
					outItems = g.processSubsets( items, 2 );
					for(List<String> lst: outItems){
						itemPair = lst.get(0);// + " " + lst.get(1);
						for(int j=1;j<no_pairs;j++){
							itemPair += (" " + lst.get(j)); 
						}
						itemList.set(itemPair);
						context.write(itemList, countItemList);
					}
					
					
				}
			}
			
		}
		
		public void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException{
			// take the value from the command line
			JobConf jobConf = (JobConf)context.getConfiguration();
			int no_pairs = Integer.parseInt(jobConf.get("numPairs"));
			

			// input line
			String line = value1.toString();
			StringTokenizer itr = new StringTokenizer(line);
			
			//buildOutput(no_pairs, itr, items, context);
			buildOutputWithSort(no_pairs, itr, context);
		}
		
	}
	
	
	/**
	 * MyReducer
	 * input: (key2, <value2>)
	 * output: <(key3, value3)>
	 * 
	 * @author Dhanesh
	 *
	 */
	protected static class MarketBasketReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		// output value3: total counted items paired
		private static final IntWritable totalItemsPaired = new IntWritable();
		
		public void reduce(Text key2, Iterable<IntWritable> values2, Context context) throws IOException, InterruptedException{
			int sum = 0;
			Iterator<IntWritable> iter = values2.iterator();
			while(iter.hasNext()){
				sum += iter.next().get();
			}
			totalItemsPaired.set(sum);
			context.write(key2, totalItemsPaired);
		}
	}
	}

}