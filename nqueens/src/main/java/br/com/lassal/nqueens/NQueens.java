/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.nqueens;

import br.com.lassal.nqueens.output.NQueensConsoleOutput;
import br.com.lassal.nqueens.output.NQueensCountOutput;
import br.com.lassal.nqueens.output.NQueensOutput;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Lucalves33
 */
public class NQueens {

    private static final String usageMsg = "NQueensSolver wrong usage!\n" +
                                           "Please use NQueensSolver (params) numberOfQueens\n" +
                                           "\n================================================\n" +
                                           "OPTIONS:\n" +
                                           "-threads=<number of threads>   informs the number of threads will execute the processing. Default 1 thread\n" +
                                           "-output={console|json|count}   informs the type of output. Console show the board solutions on console, JSON outputs in JSON in the current folder, count only counts solutions and time to process\n" ;
    //        + "{-threads=4} -output={console|json|count} <board size> "
    private static final int NUMBER_POSITIONS_SOLSTEAM = 4;
    
    private static int threadPoolSize = 1;
    private static NQueensOutput defaultOutput = new NQueensCountOutput();
    private static int queensNumber = 1;
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {
        
        try{
            if(args.length < 1){
                System.out.println(NQueens.usageMsg);
                System.exit(1);
            }
            interpretParams(args);
            if(NQueens.defaultOutput == null){
                System.out.println("Sorry! :-/\n OUTPUT option not implemented, try call it output option count");
                System.exit(1);
            }
        }
        catch(Exception ex){
            System.out.println(NQueens.usageMsg);
            System.exit(1);
        }
        
        NQueens.defaultOutput.setStartProcessing();
        if(NQueens.threadPoolSize == 1 || NQueens.queensNumber < 6){
            NQueens.solveSingleThread();
        }
        else{
            NQueens.solveMultiThread(NQueens.threadPoolSize, (short) NQueens.queensNumber);
        }
        NQueens.defaultOutput.setEndProcessing();
        
        NQueens.defaultOutput.showSolutionsCountAndTime();
    }
    
    private static void interpretParams(String[] params){
        if(params.length > 1){
            for(int i=0; i < 2 || i < params.length; i++){
                String[] paramValue = params[i].split("=");
                if(paramValue.length == 2){
                    if(paramValue[0].startsWith("-threads")){
                        NQueens.threadPoolSize = Integer.parseInt(paramValue[1]);
                    }
                    else if(paramValue[0].startsWith("-output")){
                        
                        if(paramValue[1].toLowerCase().equals("console")){
                            NQueens.defaultOutput = new NQueensConsoleOutput();
                        }
                        else if(paramValue[1].toLowerCase().equals("json")){
                            NQueens.defaultOutput = null;
                        }
                        else if(paramValue[1].toLowerCase().equals("count")){
                            NQueens.defaultOutput = new NQueensCountOutput();
                        }
                    }
                }
            }
        }
        NQueens.queensNumber = Integer.parseInt(params[params.length-1]);
    }
    
    private static void solveSingleThread(){
        PuzzleSolver solver = new PuzzleSolver((short)NQueens.queensNumber, NQueens.defaultOutput);
        solver.solve();
        
    }
    
    private static void solveMultiThread(int numThreads, short boardSize) throws InterruptedException{
        Thread[] threadPool = new Thread[numThreads];
        ConcurrentLinkedQueue<short[]> partialResults = new ConcurrentLinkedQueue<short[]>();
        
        for (short a = 0; a < boardSize; a++) {
                for (short b = 0; b < boardSize; b++) {
                        for (short c = 0; c < boardSize; c++) {
                                for (short d = 0; d < boardSize; d++) {
                                    if (!(a == b || a == c || a == d || b == c || b == d || c == d
                                            || a == (b + 1) || a == (b - 1) || a == (c + 2) || a == (c - 2)
                                            || a == (d + 3) || a == (d - 3) || b == (c + 1) || b == (c - 1)
                                            || b == (d + 2) || b == (d - 2) || c == (d + 1) || c == (d - 1))) {
                                        short[] partialSolution = {a, b, c, d};
                                        partialResults.add(partialSolution);
                                    }
                                }
                        }
                }
        }

        for(int i=0 ; i < numThreads; i++){
            threadPool[i] = new Thread(new PuzzleSolver(boardSize, NQueens.defaultOutput, partialResults));
            threadPool[i].start();
        }
        
        for(int i=0; i < numThreads; i++){
            threadPool[i].join();
        }
    }

   
}
