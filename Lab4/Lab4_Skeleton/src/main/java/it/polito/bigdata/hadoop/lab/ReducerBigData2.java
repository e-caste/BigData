package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type

    ArrayList<Float> scores = new ArrayList<>();

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<FloatWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	for (FloatWritable value : values) {
    	    scores.add(value.get());
        }
    	float scoresMean = (float) scores.stream().reduce((float) 0, Float::sum) / scores.size();
    	context.write(key, new FloatWritable(scoresMean));
    }
}
