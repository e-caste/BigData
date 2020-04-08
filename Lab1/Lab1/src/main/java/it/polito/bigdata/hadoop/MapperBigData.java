package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] lines = value.toString().split("\n");
            ArrayList<String> _2Grams = new ArrayList<>();
            for (String line: lines) {
                String[] words = line.split("\\s+");
                for (int i = 0; i < words.length - 1; ++i) {
                    words[i] = words[i] + " " + words[i+1];
                }
                _2Grams.addAll(Arrays.asList(words).subList(0, words.length - 1));
            }

//            System.out.println(_2Grams);

            // Iterate over the set of words
            for(String _2Gram : _2Grams) {
            	// Transform word case
                String cleaned2Gram = _2Gram.toLowerCase();
                
                // emit the pair (word, 1)
                context.write(new Text(cleaned2Gram),
                		      new IntWritable(1));
            }
    }
}
