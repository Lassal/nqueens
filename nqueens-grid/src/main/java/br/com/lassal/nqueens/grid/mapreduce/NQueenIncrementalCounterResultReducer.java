/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.nqueens.grid.mapreduce;

import java.math.BigInteger;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Lucalves33
 */
public class NQueenIncrementalCounterResultReducer extends Reducer<Text, Text, NullWritable, Text>{
    
    
    
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
       BigInteger partialCount = BigInteger.ZERO;
       long numPartialSolutions = 0;
       
       for(Text item : values){
           String value = item.toString();
           
           if(NQueenIncrementalCounterResultMapper.PARTIAL_SOLUTION_ID.equals(value) && numPartialSolutions < Long.MAX_VALUE){
               numPartialSolutions++;
           }
           else{
               BigInteger solutionCount = new BigInteger(value);
               partialCount = partialCount.add(solutionCount);
           }
       }
       
       context.write(NullWritable.get(), new Text(key.toString() + "=" + partialCount.toString() + ";"));
       
       if(numPartialSolutions > 0){
           context.write(NullWritable.get(), new Text(key.toString() + " NÃO FINALIZADO - EXISTEM " + numPartialSolutions + " SOLUÇÕES ABERTAS."));
       }
    }
}
