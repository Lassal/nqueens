/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.lassal.nqueens.grid.job;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Lucalves33
 */
public class GenerateSolutions extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 

        int res = ToolRunner.run(new Configuration(), new GenerateSolutions(), args);

        System.exit(res);

    }

    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Create a JobConf using the processed conf
        Job job = new Job(conf, "nqueens-gensolutions");
        job.setJarByClass(GenerateSolutions.class);

        // este job nao possui reduce tasks
        job.setNumReduceTasks(0);

        int queensNumber = Integer.parseInt(args[0]);

        this.setWorkingFolder(queensNumber, job);

        job.setMapperClass(br.com.lassal.nqueens.grid.mapreduce.NQueenPartialShotMapper.class);

        // Submit the job, then poll for progress until the job is complete
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;

    }

    /**
     * NQueens working folder structure /nqueens/board-{x}/partial/solution_X-4
     *
     * @param queensSize
     * @throws IOException
     */
    private void setWorkingFolder(int queensSize, Job job) throws IOException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        if (fs.isDirectory(new Path("/nqueens/board-" + queensSize + "/final"))) {
            System.exit(0); // ja foi processado anteriormente nao processa de novo
        }

        String lastSolution = null;
        Path partialSolDir = new Path("/nqueens/board-" + queensSize + "/partial/");
        Path inputPath = null;
        Path outputPath = null;

        if (fs.exists(partialSolDir)) {
            RemoteIterator<LocatedFileStatus> dirsFound = fs.listLocatedStatus(partialSolDir);

            while (dirsFound.hasNext()) {
                LocatedFileStatus path = dirsFound.next();
                if (lastSolution == null) {
                    lastSolution = path.getPath().getName();
                    inputPath = path.getPath();
                } else {
                    String currentDir = path.getPath().getName();
                    if (lastSolution.compareToIgnoreCase(currentDir) < 0) {
                        lastSolution = currentDir;
                        inputPath = path.getPath();
                    }
                }
            }
        }
        int currentSolutionSet = 0;
        if (inputPath == null) {
            inputPath = new Path("/nqueens/board-" + queensSize + "/seed");
            if (!fs.exists(inputPath)) {
                FSDataOutputStream seedFile = fs.create(inputPath, true);
                seedFile.writeBytes(queensSize + "#");
                seedFile.close();
            }
        }
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        if (lastSolution != null) {
            String[] solution = lastSolution.split("-");
            if (solution[0].equalsIgnoreCase("solution_" + queensSize)) {
                currentSolutionSet = Integer.parseInt(solution[1]) + 4;
                
                if(currentSolutionSet >= queensSize){
                   outputPath = new Path("/nqueens/board-" + queensSize + "/final"); 
                }
                else{
                   outputPath = new Path("/nqueens/board-" + queensSize + "/partial/solution_" + queensSize + "-" + currentSolutionSet );
                }
            }
        } else {
            outputPath = new Path("/nqueens/board-" + queensSize + "/partial/solution_" + queensSize + "-4");
        }

        // Output
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);

    }

}
