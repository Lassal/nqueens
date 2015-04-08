/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens.grid.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Lucalves33
 */
public class NQueensIncrementalSolutionCounterTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapReduceDriver;

    public NQueensIncrementalSolutionCounterTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        NQueenIncrementalCounterMapper mapper = new NQueenIncrementalCounterMapper();
        NQueenIncrementalCounterReducer reducer = new NQueenIncrementalCounterReducer();

        mapDriver = new MapDriver<LongWritable, Text, Text, Text>()
                .withMapper(mapper);
        reduceDriver = new ReduceDriver<Text, Text, NullWritable, Text>()
                .withReducer(reducer);
        ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test
    public void testMapFunction() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("5:"));
        mapDriver.withOutput(new Text("5:0,2,4,1"), new Text("5:0,2,4,1"));
        mapDriver.withOutput(new Text("5:0,3,1,4"), new Text("5:0,3,1,4"));
        mapDriver.withOutput(new Text("5:1,3,0,2"), new Text("5:1,3,0,2"));
        mapDriver.withOutput(new Text("5:1,4,0,3"), new Text("5:1,4,0,3"));
        mapDriver.withOutput(new Text("5:1,4,2,0"), new Text("5:1,4,2,0"));
        mapDriver.withOutput(new Text("5:2,0,3,1"), new Text("5:2,0,3,1"));
        mapDriver.withOutput(new Text("5:2,4,1,3"), new Text("5:2,4,1,3"));
        mapDriver.withOutput(new Text("5:3,0,2,4"), new Text("5:3,0,2,4"));
        mapDriver.withOutput(new Text("5:3,0,4,1"), new Text("5:3,0,4,1"));
        mapDriver.withOutput(new Text("5:3,1,4,2"), new Text("5:3,1,4,2"));
        mapDriver.withOutput(new Text("5:4,1,3,0"), new Text("5:4,1,3,0"));
        mapDriver.withOutput(new Text("5:4,2,0,3"), new Text("5:4,2,0,3"));

        mapDriver.runTest();
    }

    @Test
    public void testRecordParser() {
        Pattern rPattern = Pattern.compile("(\\d+):?(\\d*)=?(\\d*;?)");

        String record = "12:02413006111=1";

        Matcher matcher = rPattern.matcher(record);

        assertTrue(matcher.matches());
        assertEquals(3, matcher.groupCount());
        assertEquals("02413006111", matcher.group(2));

        assertFalse(matcher.group(3).endsWith(";"));

        Matcher startRecMatcher = rPattern.matcher("12");
        assertTrue(startRecMatcher.matches());
        assertEquals("12", startRecMatcher.group(1));
        assertEquals("", startRecMatcher.group(2));
    }

    @Test
    public void testNQueenCountRecord() {
        String rawRecord = "12:0,2,4,1,3,0,0,6=23;";
        NQueenCountRecord record = NQueenCountRecord.parse(rawRecord, 4);

        int nqueenSize = 12;
        boolean isClosed = true;
        String predicate = "12:0,2,4,1";
        int[] partialSolution = {0, 2, 4, 1, 3, 0, 0, 6};

        assertNotNull(record);
        assertEquals(nqueenSize, record.getNQueensSize());
        assertEquals(isClosed, record.isBranchSolved());
        assertEquals(predicate, record.getPredicate());
        assertArrayEquals(partialSolution, Arrays.copyOf(record.getPartialSolution(), 8));

    }

    @Test
    public void testMapFunction2ndStep() throws IOException {
//        mapDriver.withInput(new Text(), new Text("256:0,2,4,7"));
//        mapDriver.withInput(new Text("256:0,2,4,7"), new Text("256:0,2,4,7,1,3,5,10,13,15,6,18,20,22,8,25,9,28,30,11,33,12,17,37,14,40,42,16,45,47,49,19,52,54,21,57,59,61,23,64,24,67,69,26,72,27,32,76,29,79,81,31,84,86,88,34,91,93,35,96,36,39,100,38,103,105,107,109,41,43,113,115,117,44,46,121,123,125,48,128,130,132,50,135,51,138,55,53,142,144,146,148,56,58,152,154,156,60,159,161,163,62,166,63,169,171,65,174,66,71,178,68,181,183,70,186,188,190,73,193,74,196,75,199,201,203,77,206,78,209,82,80,213,215,217,219,83,85,223,225,227,87,230,232,234,89,237,90,240,242,244,254,251,249,255,252,94,98,253,95,102,92,97=0"));
        mapDriver.withInput(new LongWritable(), new Text("12:0,2,4,1"));
//        mapDriver.withInput(new Text(), new Text("5:0,3,1,4"));
//        mapDriver.withInput(new Text(), new Text("5:1,3,0,2"));
//        mapDriver.withInput(new Text(), new Text("5:1,4,0,3"));
//        mapDriver.withInput(new Text(), new Text("5:1,4,2,0"));
//        mapDriver.withInput(new Text(), new Text("5:2,0,3,1"));
//        mapDriver.withInput(new Text(), new Text("5:2,4,1,3"));
//        mapDriver.withInput(new Text(), new Text("5:3,0,2,4"));
//        mapDriver.withInput(new Text(), new Text("5:3,0,4,1"));
//        mapDriver.withInput(new Text(), new Text("5:3,1,4,2"));
//        mapDriver.withInput(new Text(), new Text("5:4,1,3,0"));
//        mapDriver.withInput(new Text(), new Text("5:4,2,0,3"));
//
//        mapDriver.withOutput(new Text("5:0,2,4,1"), new Text("5:0,2,4,1=1;"));
//        mapDriver.withOutput(new Text("5:0,3,1,4"), new Text("5:0,3,1,4=1;"));
//        mapDriver.withOutput(new Text("5:1,3,0,2"), new Text("5:1,3,0,2=1;"));
//        mapDriver.withOutput(new Text("5:1,4,0,3"), new Text("5:1,4,0,3=0;"));
//        mapDriver.withOutput(new Text("5:1,4,2,0"), new Text("5:1,4,2,0=1;"));
//        mapDriver.withOutput(new Text("5:2,0,3,1"), new Text("5:2,0,3,1=1;"));
//        mapDriver.withOutput(new Text("5:2,4,1,3"), new Text("5:2,4,1,3=1;"));
//        mapDriver.withOutput(new Text("5:3,0,2,4"), new Text("5:3,0,2,4=1;"));
//        mapDriver.withOutput(new Text("5:3,0,4,1"), new Text("5:3,0,4,1=0;"));
//        mapDriver.withOutput(new Text("5:3,1,4,2"), new Text("5:3,1,4,2=1;"));
//        mapDriver.withOutput(new Text("5:4,1,3,0"), new Text("5:4,1,3,0=1;"));
//        mapDriver.withOutput(new Text("5:4,2,0,3"), new Text("5:4,2,0,3=1;"));

        mapDriver.runTest();
    }

    @Test
    public void testReduceFunction() throws IOException{
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("12:0,2,4,1,0,0,0,0=5;"));
        values.add(new Text("12:0,2,4,1,0,0,0,1=3;"));
        values.add(new Text("12:0,2,4,1,0,0,0,2,5,5=3"));
        reduceDriver.addInput(new Text("12:0,2,4,1"), values);
        reduceDriver.addOutput(NullWritable.get(), new Text("12:0,2,4,1,0,0,0,2,5,5=3"));
        reduceDriver.addOutput(NullWritable.get(), new Text("12:0,2,4,1=8;"));
        reduceDriver.runTest();
    }
    
    @Test
    public void testMapReduce() throws IOException{

       mapReduceDriver.addInput(new LongWritable(), new Text("8"));
       List<Pair<NullWritable,Text>> firstResult = mapReduceDriver.run();
       
       MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> secondRun = 
                MapReduceDriver.newMapReduceDriver(mapReduceDriver.getMapper(), mapReduceDriver.getReducer());;
       
       for(Pair<NullWritable,Text> pair : firstResult){
           secondRun.addInput(new LongWritable(), pair.getSecond());
          //secondRun.addAll(firstResult);
       }
       
       List<Pair<NullWritable,Text>> partialResult = secondRun.run();
      
       
       NQueenIncrementalCounterResultMapper finalMapper = new NQueenIncrementalCounterResultMapper();
       NQueenIncrementalCounterResultReducer finalReducer = new NQueenIncrementalCounterResultReducer();
       MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> finalRun
                = MapReduceDriver.newMapReduceDriver(finalMapper, finalReducer);

       //finalRun.addAll(partialResult);
       for(Pair<NullWritable,Text> pair : partialResult){
           finalRun.addInput(new LongWritable(), pair.getSecond());
       }
       List<Pair<NullWritable,Text>> finalResult = finalRun.run();
       
       for(Pair<NullWritable,Text> value : finalResult){
           System.out.println(value.getFirst().toString() + " : " + value.getSecond().toString());
       }
       
    }
}
