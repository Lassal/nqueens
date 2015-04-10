/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.nqueens.output;

import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author Lucalves33
 */
public class NQueensCountOutput implements NQueensOutput{
    
    private int boardSize = -1;
    protected long solutionCount = 0;
    private Date startProcessing = null;
    private Date endProcessing = null;
    
    public synchronized void collectResult(short[] result) {
        if(this.boardSize < 0){
            this.boardSize = result.length;
        }
        this.solutionCount++;
    }

    public void showSolutionsCountAndTime() {
        long miliseconds = this.endProcessing.getTime() - this.startProcessing.getTime();
        System.out.format("Numero solucoes para NQueens(%d) %d - Tempo milisegundos %d\n", this.boardSize, this.solutionCount, miliseconds);
    }
    
    public void setStartProcessing(){
        this.startProcessing = Calendar.getInstance().getTime();
    }
    
    public void setEndProcessing(){
        this.endProcessing = Calendar.getInstance().getTime();
    }
}
