/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens.grid.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Lucalves33
 */
public class NQueenIncrementalCounterMapper extends Mapper<LongWritable, Text, Text, Text> {

    public final static int QTD_POSICOES_PRESOLUCAO = 4;

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {

        // obter o tempo da propriedade do Hadoop de tempo de execução
        long tempoLimite = 9 * 60 * 1000; // minutos * segundos * milesimos

        NQueenCountRecord record = NQueenCountRecord.parse(value.toString(), NQueenIncrementalCounterMapper.QTD_POSICOES_PRESOLUCAO);

        if (!record.hasPredicate()) {
            this.mapeiaInicioSolucao(record.getNQueensSize(), context);
        } else {
            this.resolveNQueens(record, tempoLimite, context);
        }
    }

    private void mapeiaInicioSolucao(int nQueensSize, Context mapContext) throws IOException, InterruptedException {
        int qtdPosicoesPreSolucao = NQueenIncrementalCounterMapper.QTD_POSICOES_PRESOLUCAO;

        int[] boardRow = new int[qtdPosicoesPreSolucao];

        int y = 0;
        boardRow[y] = -1;
        while (y >= 0) {
            do {
                boardRow[y]++;
            } while ((boardRow[y] < nQueensSize) && unsafe(boardRow, y));
            if (boardRow[y] < nQueensSize) {
                if (y < (qtdPosicoesPreSolucao - 1)) {
                    boardRow[++y] = -1;
                } else {
                    Text keyValue = new Text(NQueenCountRecord.writePredicate(nQueensSize, boardRow));
                    mapContext.write(keyValue, keyValue);
               //     String key = String.format("%d:%d,%d,%d,%d", nQueensSize, boardRow[0], boardRow[1], boardRow[2], boardRow[3]);
               //     System.out.format("mapDriver.withOutput(new Text(\"%s\"), new Text(\"%s\"));\n", keyValue.toString(), keyValue.toString());
                }
            } else {
                y--;
            }
        }

    }

    private void resolveNQueens(NQueenCountRecord record, long tempoLimite, Context mapContext) throws IOException, InterruptedException {
        int qtdPosicoesPreSolucao = NQueenIncrementalCounterMapper.QTD_POSICOES_PRESOLUCAO;
        Date startProcessTime = Calendar.getInstance().getTime();
        if (record.isBranchSolved()) {
            mapContext.write(new Text(record.getPredicate()), new Text(record.toString()));
        } else {
            int[] boardRow = record.getPartialSolution();

            long numPassos = 0;
            int numPositionsSolved = record.getNumPositionsSolved();
            int y = numPositionsSolved;
            boardRow[y] = -1;
            int tamAgrupador = record.getNQueensSize() - 4;
            tamAgrupador = tamAgrupador < NQueenIncrementalCounterMapper.QTD_POSICOES_PRESOLUCAO ? NQueenIncrementalCounterMapper.QTD_POSICOES_PRESOLUCAO : tamAgrupador;
            int[] agrupador = null;
            int numSolucoesAgrupador = 0;
            long elapsedTime = 0;

            while (y >= qtdPosicoesPreSolucao && elapsedTime < tempoLimite) {
                do {
                    boardRow[y]++;
                    numPassos++;
                } while ((boardRow[y] < record.getNQueensSize()) && unsafe(boardRow, y));

                if (boardRow[y] < record.getNQueensSize()) {
                    if (y < (record.getNQueensSize() - 1)) {
                        boardRow[++y] = -1;
                    } else {

                        int[] agrupadorAtual = Arrays.copyOf(boardRow, tamAgrupador);
                        if (agrupador == null) {
                            agrupador = agrupadorAtual;
                        }
                        if (!Arrays.equals(agrupador, agrupadorAtual)) {
                            if (numSolucoesAgrupador > 0) {
                                Text key = new Text(record.getPredicate());
                                Text value = new Text(NQueenCountRecord.writeRecord(record.getNQueensSize(), agrupador, numSolucoesAgrupador, true));
                                mapContext.write(key, value);
                             //   System.out.println(value.toString());
                            }
                            agrupador = agrupadorAtual;
                            numSolucoesAgrupador =0;
                        }
                        numSolucoesAgrupador++;
                    }
                } else {
                    boardRow[y] = -1;
                    y--;
                }

                if ((numPassos % 1000) == 0) {
                    Date now = Calendar.getInstance().getTime();
                    elapsedTime = now.getTime() - startProcessTime.getTime();
                }
            }
            if (y >= qtdPosicoesPreSolucao) {
                Text key = new Text(record.getPredicate());
                Text value = new Text(NQueenCountRecord.writeRecord(record.getNQueensSize(), boardRow, numSolucoesAgrupador, false));
                mapContext.write(key, value);
                //System.out.println(value.toString());
            } else if(agrupador != null && numSolucoesAgrupador > 0){
                Text key = new Text(record.getPredicate());
                Text value = new Text(NQueenCountRecord.writeRecord(record.getNQueensSize(), agrupador, numSolucoesAgrupador, true));
                mapContext.write(key, value);
               // System.out.println(value.toString());
            }

        }
    }

    private boolean unsafe(int[] bRow, int y) {
        int x = bRow[y];
        for (int i = 1; i <= y; i++) {
            int t = bRow[y - i];
            if (t == x
                    || t == x - i
                    || t == x + i) {
                return true;
            }
        }

        return false;
    }
}

/*

 Sem predicado
 X >= 0 AND X < 4
 
 Com predicado
 AGRUPADOR = PREDICADO
 COUNT_AGRUPADOR = 0
 WHILE tempo de execução for o do limite, Ntentativas < MaxTentativas (2^20 = 1048576)
 X >=4
 IF X == N-4 AND boardRow_UntilX != AGRUPADOR
 Salva agrupador atual + counter : key:predicado, valor:boardRow[X posicoes]={count}
 AGRUPADOR = Cria agrupador boardRow_UntilX
 COUNT_AGRUPADOR = 0
 -----------------------
 Para cada solucao encontrada
 COUNT_AGRUPADOR++
 :ENDWHILE
   
 IF X < 4 
 Salva agrupador atual + counter : key:predicado, valor:boardRow[X posicoes]={count}
 IF X > NUMPOSPREDICADO
 Salva agrupador atual + counter : key:predicado, valor:boardRow[X posicoes]={count} >>> SETUP N-4 posições a mais no startup do board


 */
