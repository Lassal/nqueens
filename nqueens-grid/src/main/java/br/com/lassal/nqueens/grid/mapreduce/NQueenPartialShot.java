/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens.grid.mapreduce;

/**
 *
 * @author Lucalves33
 */
public class NQueenPartialShot {

    private int columnSize = 0;
    private int[] prefix = null;
    private String rawPrefix = null;

    public NQueenPartialShot(String record) {
        this.parseRecord(record);
    }

    /**
     * 25:11,6,3,20
     *
     * @param record
     */
    private void parseRecord(String record) {

        if (record != null) {
            System.out.println("Record informado: " + record);
            if (record.endsWith("#")) {
                Integer size = com.google.common.primitives.Ints.tryParse(record.substring(0, record.length() - 1));
                this.columnSize = size == null ? 0 : size.intValue();
            } else {

                String[] sizeAndValues = record.split(":");

                if (sizeAndValues.length == 2) {
                    Integer size = com.google.common.primitives.Ints.tryParse(sizeAndValues[0]);

                    if (size != null) {
                        this.columnSize = size;

                        String[] valuesRaw = sizeAndValues[1].split(",");

                        if (valuesRaw.length <= this.getColumnSize()) {
                            int[] values = new int[valuesRaw.length];

                            for (int i = 0; i < valuesRaw.length; i++) {
                                values[i] = (int) com.google.common.primitives.Ints.tryParse(valuesRaw[i]);
                            }

                            this.prefix = values;
                            this.rawPrefix = record;
                        }
                    }
                }
            }
        }

    }

    /**
     * @return the columnSize
     */
    public int getColumnSize() {
        return columnSize;
    }

    /**
     * @return the prefix
     */
    public int[] getPrefix() {
        return prefix;
    }

    /**
     * @return the rawPrefix
     */
    public String getRawPrefix() {
        return rawPrefix;
    }

    public String getNewPrefix() {
        return rawPrefix != null ? rawPrefix + "," : this.columnSize + ":";
    }

    public boolean hasPrefix() {
        return this.prefix != null && this.prefix.length > 0;
    }

    public int getNumColToRun(int maxRun) {
        int numColToRun = maxRun;

        if (this.hasPrefix()) {
            numColToRun = this.columnSize - this.prefix.length;

            if (numColToRun > maxRun) {
                numColToRun = maxRun;
            }
        }
        return numColToRun;
    }
    
    public boolean isFinalRun(int maxRun){
        if(this.hasPrefix()){
            int possibleRuns = this.prefix.length + maxRun;
            
            return possibleRuns >= this.columnSize;
        }
        else{
            return maxRun >= this.columnSize;
        }
    }
}
