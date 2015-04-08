/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.mrunit.example.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Lucalves33
 */
public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private Text status = new Text();
    private final static IntWritable addOne = new IntWritable(1);
 
  /**
   * Returns the SMS status code and its count
   */
    
  protected void map(LongWritable key, Text value, Context context)
      throws java.io.IOException, InterruptedException {
 
    //655209;1;796764372490213;804422938115889;6 is the Sample record format
    String[] line = value.toString().split(";");
    // If record is of SMS CDR
    if (Integer.parseInt(line[1]) == 1) {
      status.set(line[4]);
      context.write(status, addOne);
      context.write(new Text("Z"), addOne);
    }
  }
}
