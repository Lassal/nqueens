/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens.grid.mapreduce;

import java.math.BigInteger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Lucalves33
 */
public class NQueenIncrementalCounterReducer extends Reducer<Text, Text, NullWritable, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        BigInteger partialCounter = BigInteger.valueOf(0);
        String lastResult = "";

        for (Text value : values) {
            NQueenCountRecord record = NQueenCountRecord.parse(value.toString(), NQueenIncrementalCounterMapper.QTD_POSICOES_PRESOLUCAO);

            if (record.isBranchSolved()) {
                if (record.getPartialSolutionText().compareTo(lastResult) > 0) {
                    lastResult = record.getPartialSolutionText();
                }
                partialCounter = partialCounter.add(record.getSolutionsCount());
            } else {
                context.write(NullWritable.get(), value);
            }
        }

        if (partialCounter.compareTo(BigInteger.valueOf(0)) > 0) {
            String sumValue = key.toString() + "=" + partialCounter.toString() + ";";
            context.write(NullWritable.get(), new Text(sumValue));
        }
    }

}
