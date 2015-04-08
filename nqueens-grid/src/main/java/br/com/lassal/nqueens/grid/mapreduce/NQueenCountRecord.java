/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.nqueens.grid.mapreduce;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Lucalves33
 */
public class NQueenCountRecord {

    private static Pattern recordPtn = Pattern.compile("(\\d+):?([0-9,]*)=?(\\d*;?)");
    
    static NQueenCountRecord parse(String record, int predicateSize) {
        Matcher matchRecord = recordPtn.matcher(record);
        
        if(matchRecord.matches()){
          NQueenCountRecord nqr = new NQueenCountRecord(record, matchRecord.group(1), matchRecord.group(2), matchRecord.group(3));
          nqr.predicateSolutionSize = predicateSize;
          return nqr;
        }
        return null;
    }
    
    public static String writePredicate(int nQueenSize, int[] values){
        StringBuilder predBuffer = new StringBuilder(nQueenSize+ ":");
        for(int i=0; i < values.length; i++){
            if(values[i] >= 0){
                if(i > 0){
                  predBuffer.append(",");
                }
                predBuffer.append(values[i]);
            }
        }
        return predBuffer.toString();
    }
    
    public static String writeRecord(int nQueenSize, int[] values, int solutionCount, boolean branchFinished){
        String predicate = writePredicate(nQueenSize, values);
        
        String record = predicate + "=" + solutionCount;
        
        return branchFinished ? record + ";" : record;
    }
    
    //nqueenSolution
    //predicado
    //ultimoPredicado
    //solucoes encontradas
    //exemplo de registro: {numero de rainhas}:[02413]={numeroSolucoes}
    // exemplo >> 12:02413001=4
    // exemplo >> 12:02413002=3
    // exemplo >> 12:02413003=6
    // exemplo >> 12:02413004=5
    // exemplo >> 12:02413005=2
    // exemplo >> 12:02413006111=1
    //======= saida após combiner
    // exemplo >> 12:02413005=20** 
    // exemplo >> [12:02413006]12:02413006111=21 >> [12:0241]12:0,2,4,1,3,0,0,6=23*
    // exemplo >> [12:0241]12:02413007
    // :::>>>> APOS PROCESSAMENTO
    // [12:0241]12:02413006=23*
    // [12:0241]12:02413007=5*
    // [12:0241]12:0241301109=27*
    // [12:0241]12:02413012
    // :::>>>> APOS REDUCER
    // [12:0241]12:0241301109=55* (12:02413007=28 + 12:0241301109)
    // [12:0241]12:02413012
    // exemplo >> [12:0241]12:0241
    // ------->>> 12:0241300 
    //Inicio: prefixo:02413006
    
    //99:0241300

    private int predicateSolutionSize = -1;
    private String rawRecord = null;
    private int nqueenSize = -1;
    private String partialSolutionText = null;
    private int[] partialSolution = null;
   // private long solutionsCount = -1;
    private BigInteger solutionsCount = BigInteger.valueOf(-1);
    private boolean isCountClosed = false;
    private int numPositionsSolved = 0;
    
    private NQueenCountRecord(String rawRecord, String nqueenSize, String partialSolution, String solutionCount){
        this.rawRecord = rawRecord;
        this.nqueenSize = Integer.parseInt(nqueenSize);
        
        if(partialSolution != null && !partialSolution.isEmpty()){
            this.partialSolutionText = partialSolution;
            
            if(solutionCount != null && !solutionCount.isEmpty()){
                String count = solutionCount;
                
                if(solutionCount.endsWith(";")){
                    this.isCountClosed = true;
                    count = solutionCount.substring(0, solutionCount.length()-1);
                }
                this.solutionsCount = new BigInteger(count);
            }
        }
        this.parsePartialSolution();
    }
    
    public boolean hasPredicate() {
        return this.partialSolutionText != null && !this.partialSolutionText.isEmpty();
    }

    private String predicate = null;
    public String getPredicate(){
        if(predicate == null){
          this.predicate = NQueenCountRecord.writePredicate(this.nqueenSize, Arrays.copyOf(this.partialSolution, this.predicateSolutionSize));
        }
        return this.predicate;
    }
    
    public int getNQueensSize(){
        return this.nqueenSize;
    }

    @Override
    public String toString(){
        return this.rawRecord;
    }
    
    private void parsePartialSolution() {
        if(this.partialSolutionText != null && !this.partialSolutionText.isEmpty()){
            String[] positions = this.partialSolutionText.split(",");
            
            this.partialSolution = new int[this.nqueenSize];
            
            for(int i=0; i < this.partialSolution.length; i++){
                if(i < positions.length){
                    this.partialSolution[i] = Integer.parseInt(positions[i]);
                    if(this.partialSolution[i] > -1){
                       this.numPositionsSolved++;
                    }
                }
                else{
                    this.partialSolution[i] = -1;
                }
            }
        }
    }

    public boolean isBranchSolved() {
        return this.isCountClosed;
    }

    public int[] getPartialSolution() {
        return this.partialSolution;
    }
    
    public int getNumPositionsSolved(){
        return this.numPositionsSolved;
    }
    
    public BigInteger getSolutionsCount(){
        return this.solutionsCount;
    }
    
    public String getPartialSolutionText(){
        return this.partialSolutionText;
    }
}
