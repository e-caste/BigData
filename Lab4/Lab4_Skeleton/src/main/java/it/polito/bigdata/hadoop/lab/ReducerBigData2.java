package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,               // Input key type
                DoubleWritable,     // Input value type
                Text,               // Output key type
                DoubleWritable> {   // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<DoubleWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */

        int numRatings = 0;
        double totRatings = 0;
        double avg;

        // Iterate over the set of values
        for (DoubleWritable rating : values) {
            numRatings++;
            totRatings = totRatings + rating.get();
        }

        avg = totRatings / (double) numRatings;


        context.write(new Text(key), new DoubleWritable(avg));
    }
}
