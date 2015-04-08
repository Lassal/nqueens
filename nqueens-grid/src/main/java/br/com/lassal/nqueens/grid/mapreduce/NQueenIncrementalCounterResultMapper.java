/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens.grid.mapreduce;

import java.math.BigInteger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Lucalves33
 */
public class NQueenIncrementalCounterResultMapper extends Mapper<LongWritable, Text, Text, Text> {
    public static final String PARTIAL_SOLUTION_ID = "PARTIALSOL";
     
    
    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {

        NQueenCountRecord record = NQueenCountRecord.parse(value.toString(), 1);

        if (record.isBranchSolved()) {
            Text outKey = new Text(record.getPredicate());
            Text outValue = new Text(record.getSolutionsCount().toString());
            context.write(outKey, outValue);
        } else {
            Text outKey = new Text(record.getPredicate());
            Text outValue = new Text(NQueenIncrementalCounterResultMapper.PARTIAL_SOLUTION_ID);
            context.write(outKey, outValue);
            
            if (record.getSolutionsCount().compareTo(BigInteger.ZERO) > 0) {
                outValue = new Text(record.getSolutionsCount().toString());
                context.write(outKey, outValue);
            }
        }
    }
}
