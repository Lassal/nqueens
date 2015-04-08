/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens.grid.mapreduce;

import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Lucalves33
 */
public class NQueenPartialShotMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text nqueenShot = new Text();
    private final static NullWritable nullValue = NullWritable.get();
    private final static int SliceSize = 4;
    public final static String NQueenRowSize_PROP = "nqueen.rowsize";

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {

        NQueenPartialShot record = new NQueenPartialShot(value.toString());

        //   String rowSize = context.getConfiguration().get(NQueenPartialShotMapper.NQueenRowSize_PROP);
        String predicado = record.getNewPrefix();
        // predicado = predicado!= null ? predicado + "," : (rowSize + ":");

        int numIter = record.getNumColToRun(SliceSize);

        boolean isFinalRun = record.isFinalRun(SliceSize);

        if (!isFinalRun && record.hasPrefix() && record.getPrefix().length >= (SliceSize * 2)) {
            if (this.isPositionInvalid(record.getPrefix())) {
                return; // ignora esta linha 
            }
        }

        for (int a = 0; a < record.getColumnSize(); a++) {
            if (numIter > 1) {
                for (int b = 0; b < record.getColumnSize(); b++) {
                    if (numIter > 2) {
                        for (int c = 0; c < record.getColumnSize(); c++) {
                            if (numIter > 3) {
                                for (int d = 0; d < record.getColumnSize(); d++) {
                                    if (!(a == b || a == c || a == d || b == c || b == d || c == d
                                            || a == (b + 1) || a == (b - 1) || a == (c + 2) || a == (c - 2)
                                            || a == (d + 3) || a == (d - 3) || b == (c + 1) || b == (c - 1)
                                            || b == (d + 2) || b == (d - 2) || c == (d + 1) || c == (d - 1)
                                            || (isFinalRun && this.isPositionInvalid(record.getPrefix(), a, b, c, d)))) {
                                        nqueenShot.set(String.format("%s%s,%s,%s,%s", predicado, a, b, c, d));
                                        context.write(nqueenShot, nullValue);
                                    }
                                }
                            } else {
                                if (!(a == b || a == c || b == c
                                        || a == (b + 1) || a == (b - 1) || a == (c + 2) || a == (c - 2)
                                        || b == (c + 1) || b == (c - 1)
                                        || isFinalRun && this.isPositionInvalid(record.getPrefix(), a, b, c))) {
                                    nqueenShot.set(String.format("%s%s,%s,%s", predicado, a, b, c));
                                    context.write(nqueenShot, nullValue);
                                }
                            }
                        }
                    } else {
                        if (!(a == b || a == (b - 1) || a == (b + 1)
                                || isFinalRun && this.isPositionInvalid(record.getPrefix(), a, b))) {
                            nqueenShot.set(String.format("%s%s,%s", predicado, a, b));
                            context.write(nqueenShot, nullValue);
                        }
                    }
                }
            } else {
                if (isFinalRun && this.isPositionInvalid(record.getPrefix(), a)) {
                    nqueenShot.set(predicado + a);
                    context.write(nqueenShot, nullValue);
                }
            }
        }

    }

    private boolean isPositionInvalid(int[] currentPositions, int... newPositions) {

        int[] boardRow = null;

        if (currentPositions != null) {
            boardRow = Arrays.copyOf(currentPositions, currentPositions.length + newPositions.length);
            int newPosIndex = currentPositions.length;
            for (int qp : newPositions) {
                boardRow[newPosIndex++] = qp;
            }
        } else {
            boardRow = newPositions;
        }

        for (int a = 0; a < (boardRow.length - 1); a++) {
            int x = boardRow[a];
            for (int b = (a + 1); b < boardRow.length; b++) {
                int t = boardRow[b];
                int distancia = b - a;
                if (t == x || t == (x - distancia) || t == (x + distancia)) {
                    return true;
                }
            }
        }

        return false;
    }
}
