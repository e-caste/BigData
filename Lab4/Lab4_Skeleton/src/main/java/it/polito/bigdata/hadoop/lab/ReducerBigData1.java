package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,           // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        /* Implement the reduce method */

        HashMap<String, Double> productsRatings = new HashMap<>();

        int numRatings = 0;
        double totRatings = 0;

        String productId = "";
        double rating;
        double avg;

        // Iterate over the set of values and store them locally
        // because two iterations are needed. Since the number of rating
        // per user is a small set we can keep it in main memory
        for (Text productIdRating : values) {

            // Extract productId and rating from value
            // productId:rating
            String[] fields = productIdRating.toString().split(":");

            productId = fields[0];
            rating = Double.parseDouble(fields[1]);

            productsRatings.put(productId, rating);

            numRatings++;
            totRatings = totRatings + rating;

        }

        avg = totRatings / (double) numRatings;

        for (Entry<String, Double> pair : productsRatings.entrySet()) {
            productId = pair.getKey();
            rating = pair.getValue();

            context.write(new Text(productId), new DoubleWritable(rating - avg));
        }
    }
}
