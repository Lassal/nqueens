/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens.output;

/**
 *
 * @author lassal
 */
public class NQueensConsoleOutput extends NQueensCountOutput implements NQueensOutput{
    
    @Override
    public synchronized void collectResult(short[] result) { 
        super.collectResult(result);
        this.printSolution(result);
    }
    
    private void printSolution(short[] result){
        int boardSize = result.length;
        
        System.out.println("::::::::::::::::::: Solution #" + this.solutionCount);
        for(int i=0; i < boardSize; i++){
            this.printSolutionRow(boardSize, result[i]);
        }
        
        System.out.println();
    }
    
    private void printSolutionRow(int boardSize, short queenPosition){
        int rowSize = (boardSize*2);
        char[] output = new char[rowSize];
        
        for(int i = 0; i < boardSize; i++){
            output[i*2] = i == queenPosition ? 'Q' : '-';
            output[(i*2)+1] = ' ';
        }
        System.out.println(output);
    }
}
