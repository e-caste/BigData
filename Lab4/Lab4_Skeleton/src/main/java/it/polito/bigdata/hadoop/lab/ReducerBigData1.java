package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type

//    HashMap<String, Integer> userScoreMap = new HashMap<>();
    ArrayList<Integer> productScores = new ArrayList<>();
    float productScoresMean;

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        for (Text value : values) {
            productScores.add(Integer.parseInt(value.toString().split(",")[1]));
        }
        productScoresMean = (float) (productScores.stream().reduce(0, Integer::sum) / productScores.size());
        for (Text value : values) {
            float productScoreNormalized = (float) Integer.parseInt(value.toString().split(",")[1]) - productScoresMean;
            context.write(key, new Text(value.toString().split(",")[0] + "," + productScoreNormalized));
        }
    }
}
