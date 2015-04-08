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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Lucalves33
 */
public class NQueenCounter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 

        int res = ToolRunner.run(new Configuration(), new NQueenCounter(), args);

        System.exit(res);

    }

    /**
     * Forma de chamada
     * <> {numero de rainhas} {diretorio raiz} -F
     *
     * @param strings
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Create a JobConf using the processed conf
        Job job = new Job(conf, "nqueens-counter");
        job.setJarByClass(NQueenCounter.class);

        int queensNumber = Integer.parseInt(args[0]);
        String workingFolder = args.length >= 2 ? args[1] : null;
        boolean isFinal = args.length >= 3 && "-F".equals(args[2]) ? true : false;

        Path sourcePath = this.setWorkingFolder(queensNumber, workingFolder, isFinal, job);
        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.Text.class);
        
        if (isFinal) {
            job.setMapperClass(br.com.lassal.nqueens.grid.mapreduce.NQueenIncrementalCounterResultMapper.class);
            job.setReducerClass(br.com.lassal.nqueens.grid.mapreduce.NQueenIncrementalCounterResultReducer.class);
        } else {
            job.setMapperClass(br.com.lassal.nqueens.grid.mapreduce.NQueenIncrementalCounterMapper.class);
            job.setReducerClass(br.com.lassal.nqueens.grid.mapreduce.NQueenIncrementalCounterReducer.class);
        }

        // Submit the job, then poll for progress until the job is complete
        boolean result = job.waitForCompletion(true);
        
        if(sourcePath != null){
            FileSystem fs = FileSystem.get(conf);
            fs.delete(sourcePath, true);
        }
        
        return result ? 0 : 1;

    }

    private Path setWorkingFolder(int queensSize, String workingFolder, boolean isFinal, Job job) throws IOException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path returnPath = null;
        
        if (workingFolder == null) {
            workingFolder = "";
        }

        Path partialSolDir = new Path(workingFolder + "/nqueens/board-" + queensSize + "/partial/");
        Path inputPath = null;
        Path outputPath = null;
        String nextRunPath = "run_1";

        if (fs.exists(partialSolDir)) {
            RemoteIterator<LocatedFileStatus> dirsFound = fs.listLocatedStatus(partialSolDir);
            String lastRunPath = null;
            Path lastPath = null;
            
            while(dirsFound.hasNext()){
                LocatedFileStatus dir = dirsFound.next();
                
                if(dir.isDirectory()){
                    if(lastRunPath == null || dir.getPath().getName().compareTo(lastRunPath) > 0){
                        lastPath = dir.getPath();
                        lastRunPath = lastPath.getName();
                    }
                }
            }
            if(lastRunPath != null){
                String[] runParts = lastRunPath.split("_");
                int lastRun = Integer.parseInt(runParts[1]);
                nextRunPath = runParts[0] + "_" + (++lastRun);
                inputPath = lastPath;
            }
            
        }
        if (inputPath == null) {
            inputPath = new Path(workingFolder + "/nqueens/board-" + queensSize + "/seed");
            if (!fs.exists(inputPath)) {
                FSDataOutputStream seedFile = fs.create(inputPath, true);
                seedFile.writeBytes(queensSize + ":");
                seedFile.close();
            }
        }
        else{
            returnPath = inputPath;
        }
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        if (isFinal) {
            outputPath = new Path(workingFolder + "/nqueens/board-" + queensSize + "/final");
        } else {
            outputPath = new Path(workingFolder + "/nqueens/board-" + queensSize + "/partial/" + nextRunPath);
        }

        // Output
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        return returnPath;
    }

}
