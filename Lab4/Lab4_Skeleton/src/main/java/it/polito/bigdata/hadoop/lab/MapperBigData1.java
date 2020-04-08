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
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
        String[] tmp = value.toString().split("\n");
        // skip description line
        String[] lines = Arrays.copyOfRange(tmp, 1, tmp.length);
        for (String line : lines) {
            String[] strings = line.split(",");
            String productID = strings[1];
            String userID = strings[2];
            String score = strings[6];
//            System.out.println(userID + productID + "," + score);
            context.write(new Text(userID), new Text(productID + "," + score));
        }
    }
}
