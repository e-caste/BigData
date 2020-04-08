package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,         // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
            NullWritable key, // Input key type
            Iterable<WordCountWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	TopKVector<WordCountWritable> top100 = new TopKVector<>(100);
        for (WordCountWritable topX : values) {
            top100.updateWithNewElement(new WordCountWritable(topX));
//            System.out.println(topX.getWord() + " " + topX.getCount());
        }
        for (WordCountWritable topX : top100.getLocalTopK()) {
            context.write(new Text(topX.getWord()), new IntWritable(topX.getCount()));
        }
    }
}
