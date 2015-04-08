/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.mrunit.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import br.com.lassal.mrunit.example.mapreduce.SMSCDRMapper;
import br.com.lassal.mrunit.example.mapreduce.SMSCDRReducer;

/**
 *
 * @author Lucalves33
 */
public class SMSCDRMapperReducerTest {
    
  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
 
  @Before
  public void setUp() {
    SMSCDRMapper mapper = new SMSCDRMapper();
    SMSCDRReducer reducer = new SMSCDRReducer();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>()
                    .withMapper(mapper);
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                       .withReducer(reducer);
            ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 
  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "655209;1;796764372490213;804422938115889;6"));
    mapDriver.withOutput(new Text("6"), new IntWritable(1));
    mapDriver.withOutput(new Text("Z"), new IntWritable(1));
    mapDriver.runTest();
  }
 
  @Test
  public void testReducer() throws IOException {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(new Text("6"), values);
    reduceDriver.withOutput(new Text("6"), new IntWritable(2));
    reduceDriver.runTest();
  }
}
