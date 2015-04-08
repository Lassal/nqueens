/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.nqueens.grid.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Lucalves33
 */
public class NQueenPartialShotMapperReducerTest {
  MapDriver<LongWritable, Text, Text, NullWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, NullWritable, Text, NullWritable> mapReduceDriver;
 
  @Before
  public void setUp() {
    NQueenPartialShotMapper mapper = new NQueenPartialShotMapper();
   // SMSCDRReducer reducer = new SMSCDRReducer();
    mapDriver = new MapDriver<LongWritable, Text, Text, NullWritable>()
                    .withMapper(mapper);
   // reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>()
   //                    .withReducer(reducer);
         //   ReduceDriver.newReduceDriver(reducer);
 //   mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, null);
  }
 
   @Test
  public void testMapper() throws IOException {
    mapDriver.getConfiguration().set(NQueenPartialShotMapper.NQueenRowSize_PROP, "4");
    mapDriver.withInput(new LongWritable(), new Text("4#"));
    mapDriver.withOutput(new Text("4:1,3,0,2"), NullWritable.get());
    mapDriver.withOutput(new Text("4:2,0,3,1"), NullWritable.get());
    mapDriver.runTest();
  }
  
  @Test
  public void testMapper2ndStep() throws IOException{
    mapDriver.withInput(new LongWritable(), new Text("6:0,2,4,1"));
   // mapDriver.withOutput(new Text(), NullWritable.get());
    mapDriver.runTest();
  }
  
  /**
   * 3,0,4,1 => 5,2
     1,3,5,0 => 2,5
     2,5,1,4 => 0,3
     4,2,0,5 => 3,1
   * 
   */
  @Test
  public void testMapper2ndStepBoard6() throws IOException{
    mapDriver.withInput(new LongWritable(), new Text("6:1,3,5,0"));
    mapDriver.withInput(new LongWritable(), new Text("6:2,5,1,4"));
    mapDriver.withInput(new LongWritable(), new Text("6:3,0,4,1"));
    mapDriver.withInput(new LongWritable(), new Text("6:4,2,0,5"));
    mapDriver.withOutput(new Text("6:1,3,5,0,2,4"), NullWritable.get());
    mapDriver.withOutput(new Text("6:2,5,1,4,0,3"), NullWritable.get());
    mapDriver.withOutput(new Text("6:3,0,4,1,5,2"), NullWritable.get());
    mapDriver.withOutput(new Text("6:4,2,0,5,3,1"), NullWritable.get());
    mapDriver.runTest();
  }

  /**
   * 3,0,4,1 => 5,2
     1,3,5,0 => 2,5
     2,5,1,4 => 0,3
     4,2,0,5 => 3,1
   * 
   */
  @Test
  public void testMapper2ndStepBoard8() throws IOException{
    mapDriver.withInput(new LongWritable(), new Text("6:1,3,5,0"));
    mapDriver.withInput(new LongWritable(), new Text("6:2,5,1,4"));
    mapDriver.withInput(new LongWritable(), new Text("6:3,0,4,1"));
    mapDriver.withInput(new LongWritable(), new Text("6:4,2,0,5"));
    mapDriver.withOutput(new Text("6:1,3,5,0,2,4"), NullWritable.get());
    mapDriver.withOutput(new Text("6:2,5,1,4,0,3"), NullWritable.get());
    mapDriver.withOutput(new Text("6:3,0,4,1,5,2"), NullWritable.get());
    mapDriver.withOutput(new Text("6:4,2,0,5,3,1"), NullWritable.get());
    mapDriver.runTest();
  }
}
