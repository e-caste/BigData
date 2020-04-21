package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text,           // Input key type
                    Text,           // Input value type
                    Text,           // Output key type
                    DoubleWritable> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
        context.write(new Text(key), new DoubleWritable(Double.parseDouble(value.toString())));
    }
}
