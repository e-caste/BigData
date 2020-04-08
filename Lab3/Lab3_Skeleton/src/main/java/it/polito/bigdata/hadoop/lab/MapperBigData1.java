package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
        String[] lines = value.toString().split("\n");
        for (String line : lines) {
            String[] tmp = line.split(",");
            // skip first element that is the user ID
            String[] products = Arrays.copyOfRange(tmp, 1, tmp.length);
//            System.out.println(products);
            if (products.length >= 2) {
                Arrays.sort(products);
                for (int i=0; i<products.length-1; ++i) {
                    for (int j=i+1; j<products.length; ++j) {
                        if (!products[i].equals(products[j])) {
                            context.write(new Text(products[i] + "," + products[j]), new IntWritable(1));
                        }
                    }
                }
            }
        }
    }
}
