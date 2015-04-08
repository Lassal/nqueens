/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens;

import br.com.lassal.nqueens.output.NQueensOutput;
import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Lucalves33
 */
public class PuzzleSolver implements Runnable {

    private int boardSize = 0;
    private short[] boardRow = null;
    private NQueensOutput outputColletor = null;
    private AbstractQueue<short[]> partialSolutionQueue = null;

    public PuzzleSolver(short boardRowSize, NQueensOutput outputColletor) {
        this.boardSize = boardRowSize;
        this.boardRow = new short[boardRowSize];
        this.outputColletor = outputColletor;
    }

    public PuzzleSolver(short boardRowSize, NQueensOutput outputColletor, ConcurrentLinkedQueue<short[]> partialSolutionQueue) {
        this(boardRowSize, outputColletor);
        this.partialSolutionQueue = partialSolutionQueue;
    }

    public void run() {
        short[] solutionSteam = this.partialSolutionQueue.poll();
        
        while(solutionSteam != null){
            this.solve(solutionSteam);
            solutionSteam = this.partialSolutionQueue.poll();
        }
        
    }

    private boolean unsafe(int y) {
        int x = this.boardRow[y];
        for (int i = 1; i <= y; i++) {
            int t = this.boardRow[y - i];
            if (t == x
                    || t == x - i
                    || t == x + i) {
                return true;
            }
        }

        return false;
    }

    public void solve() {
        this.solve(null);
    }

    private void solve(short[] solutionSteam) {
        int solSteamSize = 0;

        if (solutionSteam != null) {
            for (int i = 0; i < solutionSteam.length; i++) {
                boardRow[i] = solutionSteam[i];
            }
            solSteamSize = solutionSteam.length;
        }
        int y = solSteamSize;
        boardRow[y] = -1;

        while (y >= solSteamSize) {
            do {
                boardRow[y]++;
            } while ((boardRow[y] < this.boardSize) && unsafe(y));
            if (boardRow[y] < this.boardSize) {
                if (y < (this.boardSize - 1)) {
                    boardRow[++y] = -1;
                } else {
                    //putboard(sBoard);
                    short[] result = Arrays.copyOf(this.boardRow, this.boardSize);
                    this.outputColletor.collectResult(result);
                }
            } else {
                y--;
            }
        }

    }
}
