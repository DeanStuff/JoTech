package org.myorg;

//WordCount.java
/*
 * Copyright 2010 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;


public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, BSONObject, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{

			System.out.println("Key: " + key);
			System.out.println("Value: " + value);
			
			final StringTokenizer itr = new StringTokenizer( value.get("Model").toString() );
			while ( itr.hasMoreTokens() ){
				word.set( itr.nextToken() );
				context.write( word, one );
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private final IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main( String[] args ) throws Exception{

		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI( conf, "mongodb://localhost/test.testVehicles" );  // read from testVehicles DB
		MongoConfigUtil.setOutputURI( conf, "mongodb://localhost/test.out" );  // write out to mayank

		System.out.println( "Conf: " + conf );
		
		final Job job = new Job( conf, "word cound" );
		
		job.setJarByClass( WordCount.class );
		
		job.setMapperClass( TokenizerMapper.class );
		
		job.setCombinerClass( IntSumReducer.class );
		job.setReducerClass( IntSumReducer.class );
		
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( IntWritable.class );
		
		job.setInputFormatClass( MongoInputFormat.class );
		job.setOutputFormatClass( MongoOutputFormat.class );
		
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}
}