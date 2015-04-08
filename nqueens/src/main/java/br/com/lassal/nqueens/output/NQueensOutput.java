/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.com.lassal.nqueens.output;

/**
 *
 * @author Lucalves33
 */
public interface NQueensOutput {
    
    void collectResult(short[] result );
    void setStartProcessing();
    void setEndProcessing();
    void showSolutionsCountAndTime();
}
