/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.mrunit.example.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Lucalves33
 */
public class SMSCDRReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
