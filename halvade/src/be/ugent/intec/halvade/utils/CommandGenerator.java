/*
 * Copyright (C) 2014 ddecap
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package be.ugent.intec.halvade.utils;

import be.ugent.intec.halvade.tools.STARInstance;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author ddecap
 */
public class CommandGenerator {
    
    public static ArrayList<String> addToCommand(ArrayList<String> command, String args) {
        if(args == null || args.isEmpty()) return command;
        command.addAll(Arrays.asList(args.split("\\s+")));
        return command;
    }
    
    private static String starBin = "STAR";
    private static int STARBufferSize = 30000000; // smaller gives a bad malloc error...
    private static int SAsparseD = 8; // make smaller reference -> much faster to build, speed to align is a bit less
    private static String[] starOptions = {
        "--genomeDir", 
        "--outFileNamePrefix", 
        "--readFilesIn", 
        "--outTmpDir", 
        "--runThreadN", 
        "--twopass1readsN", 
        "--sjdbOverhang", 
        "--outStd",
        "--readFilesCommand",
        "--genomeLoad",
        "--genomeFastaFiles",
        "--sjdbFileChrStartEnd",
        "--outSAMtype",
        "--runMode",
        "--limitIObufferSize",
        "--limitGenomeGenerateRAM",
        "--genomeSAsparseD"};
    private static String[] starGenomeLoad = {
        "LoadAndExit" , "Remove", "LoadAndKeep"
    };
    private static String bwaCommand[] = {"bwa", "samxe"};
    private static String bwaTool[] = {"mem", "aln", "sampe", "samse"};
    private static String bwaOptions[] = 
        {"-p", // 0: paired (interleaved file)
         "-t" // 1: number of threads
        };
    private static String[] elPrepCommand = {"elprep"};
    private static String[] elPrepOptions = {
        "--replace-reference-sequences",
        "--filter-unmapped-reads",
        "--replace-read-group",
        "--mark-duplicates",
        "--sorting-order",
        "--clean-sam",
        "--nr-of-threads",
        "--gc-on",
        "--timed"
    };
    private static String sortOrders[] = {
        "keep", "unknown", "unsorted", "queryname", "coordinate"
    };
    private static String bedToolsCommand[] = {"bedtools"};
    private static String bedToolsOptions[] = {
        "intersect",
        "-a",
        "-b",
        "-sorted",
        "-header"
    };
    
    public static String[] bedTools(String bin, String dbsnp, String bed, String customArgs) {        
        ArrayList<String> command = new ArrayList<String>();
        if(bin.endsWith("/")) 
            command.add(bin + bedToolsCommand[0]); 
        else
            command.add(bin + "/" + bedToolsCommand[0]);
        command.add(bedToolsOptions[0]);
        command.add(bedToolsOptions[1]);
        command.add(dbsnp);
        command.add(bedToolsOptions[2]);
        command.add(bed);
//        command.add(bedToolsOptions[3]); // gives empty files??
        command.add(bedToolsOptions[4]);
        command = addToCommand(command, customArgs);
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;
    }
    /*
    
        "--replace-reference-sequences",
        "--filter-unmapped-reads",
        "--replace-read-group",
        "--mark-duplicates",
        "--sorting-order",
        "--clean-sam",
        "--nr-of-threads",
        "--gc-on",
        "--timed"
    };*/
    public static String[] elPrep(String bin, String input, String output, int threads, boolean filterUnmapped, 
           String readGroup, String refDict, String customArgs) {
        ArrayList<String> command = new ArrayList<String>();
        if(bin.endsWith("/")) 
            command.add(bin + elPrepCommand[0]); 
        else
            command.add(bin + "/" + elPrepCommand[0]);
        command.add(input);
        command.add(output);
        if(filterUnmapped)
            command.add(elPrepOptions[1]);
        if(readGroup != null) {
            command.add(elPrepOptions[2]);
            command.add(readGroup);
        }
        command.add(elPrepOptions[3]);
        if(refDict != null) {
            command.add(elPrepOptions[0]);
            command.add(refDict);
        }
        command.add(elPrepOptions[4]);
        command.add(sortOrders[0]);
        command.add(elPrepOptions[5]);
        command.add(elPrepOptions[6]);
        command.add(new Integer(threads).toString());
        command = addToCommand(command, customArgs);
//        command.add(iPrepOptions[7]); // custom garbage collection
//        command.add(iPrepOptions[8]);
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;
    }
    
    public static String[] SAMToolsView(String bin, String input, String output, String customArgs) {
        ArrayList<String> command = new ArrayList<String>();
        if(bin.endsWith("/")) 
            command.add(bin + "samtools"); 
        else
            command.add(bin + "/samtools");
        command.add("view");
        command.add("-Sb");
        command.add("-o");
        command.add(output);
        command.add(input);
        command = addToCommand(command, customArgs);
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;
    }
    
    public static String[] bwaMem(String bin,
            String bwaReferenceIndex, 
            String bwaReadsFile1, 
            String bwaReadsFile2, 
            boolean isPaired,
            boolean useSTDIN,
            int numberOfThreads, String customArgs) {
        ArrayList<String> command = new ArrayList<String>();
        if(bin.endsWith("/")) 
            command.add(bin + bwaCommand[0]); 
        else
            command.add(bin + "/" + bwaCommand[0]);
        command.add(bwaTool[0]);
        command.add(bwaReferenceIndex);
        if(isPaired)
            command.add(bwaOptions[0]);
        command.add(bwaOptions[1]);
        command.add(new Integer(numberOfThreads).toString());
        if(useSTDIN)
            command.add("/dev/stdin");
        else  {
            command.add(bwaReadsFile1);
            if(!isPaired && bwaReadsFile2 != null)
                command.add(bwaReadsFile2);
        }        
        command = addToCommand(command, customArgs);
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;        
    }
    
    public static String[] starGenomeLoad(String bin, String starGenomeDir, boolean unload) {
        ArrayList<String> command = new ArrayList<String>();
        if(bin.endsWith("/")) 
            command.add(bin + starBin); 
        else
            command.add(bin + "/" + starBin);
        command.add(starOptions[0]);
        command.add(starGenomeDir);
        command.add(starOptions[9]);
        command.add(starGenomeLoad[unload ? 1 : 0]);
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;
    }
    
    public static String[] starRebuildGenome(String bin, String newStarGenomeDir, String ref, 
            String sjdbfile, int overhang, int numberOfThreads, long mem) {
        ArrayList<String> command = new ArrayList<>();
        if(bin.endsWith("/")) 
            command.add(bin + starBin); 
        else
            command.add(bin + "/" + starBin);
        command.add(starOptions[0]);
        command.add(newStarGenomeDir);
        command.add(starOptions[1]);
        if(newStarGenomeDir.endsWith("/")) 
            command.add(newStarGenomeDir);
        else
            command.add(newStarGenomeDir + "/");        
        command.add(starOptions[10]);
        command.add(ref);
        command.add(starOptions[11]);
        command.add(sjdbfile);
        command.add(starOptions[13]);
        command.add("genomeGenerate");
        command.add(starOptions[6]);
        command.add("" + overhang);
        command.add(starOptions[4]);
        command.add("" + numberOfThreads);
        command.add(starOptions[16]);
        command.add("" + SAsparseD);
        if(mem > 0) {
            command.add(starOptions[15]);
            command.add("" + mem*1024*1024);
        }
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;
    }
    
    public static String[] starAlign(String bin, int passType,
            String starGenomeDir, 
            String outputDir,
            String readsFile1, 
            String readsFile2,
            int numberOfThreads, int overhang, int nReads, String customArgs) {
        ArrayList<String> command = new ArrayList<>();
        if(bin.endsWith("/")) 
            command.add(bin + starBin); 
        else
            command.add(bin + "/" + starBin);
        command.add(starOptions[0]);
        command.add(starGenomeDir);
        command.add(starOptions[1]);
        if(outputDir.endsWith("/")) 
            command.add(outputDir);
        else
            command.add(outputDir + "/");
        command.add(starOptions[2]);
        command.add(readsFile1);
        if(readsFile2 != null)
            command.add(readsFile2);
        command.add(starOptions[4]);
        command.add("" + numberOfThreads);
        command.add(starOptions[14]);
        command.add("" + STARBufferSize); // make default buffersize smaller so more threads are started
        if(passType == STARInstance.PASS1AND2) {
            command.add(starOptions[5]);
            command.add("" + nReads);
            command.add(starOptions[6]);
            command.add("" + overhang);
            command.add(starOptions[7]);
            command.add("SAM");
            command.add("Unsorted"); 
        } else if (passType == STARInstance.PASS1) {
            command.add(starOptions[12]);
            command.add("None");
            command.add(starOptions[9]);
            command.add(starGenomeLoad[2]);
        } else if (passType == STARInstance.PASS2) {
            command.add(starOptions[9]);
            command.add(starGenomeLoad[2]);
            command.add(starOptions[7]);
            command.add("SAM");
            command.add("Unsorted"); 
        }
        
        // for all 3 options
        if(readsFile1.endsWith(".gz")) {
            command.add(starOptions[8]);
            command.add("zcat");
        }
        command = addToCommand(command, customArgs);
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;        
    }
    
    public static String[] bwaAln(String bin,
            String bwaReferenceIndex, 
            String bwaReadsFile,
            String output,
            int numberOfThreads, String customArgs) {
        ArrayList<String> command = new ArrayList<String>();
        if(bin.endsWith("/")) 
            command.add(bin + bwaCommand[0]); 
        else
            command.add(bin + "/" + bwaCommand[0]);
        command.add(bwaTool[1]);
        if(output != null) {
            command.add("-f");
            command.add(output);
        }
        command.add(bwaOptions[1]);
        command.add(new Integer(numberOfThreads).toString());
        
        command.add(bwaReferenceIndex);
        command.add(bwaReadsFile);
        command = addToCommand(command, customArgs);
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;
    }
    
    public static String[] bwaSamXe(String bin,
            String bwaReferenceIndex, 
            String bwaSaiFile1,
            String bwaReadsFile1,
            String bwaSaiFile2,
            String bwaReadsFile2,
            boolean paired,
            int numberOfThreads, String customArgs) {
        ArrayList<String> command = new ArrayList<String>();
        if(bin.endsWith("/")) 
            command.add(bin + bwaCommand[0]); 
        else
            command.add(bin + "/" + bwaCommand[0]);
        if (paired) command.add(bwaTool[2]);
        else command.add(bwaTool[3]);
        command.add(bwaReferenceIndex);
        command.add(bwaSaiFile1);
        if(paired) command.add(bwaSaiFile2);
        command.add(bwaReadsFile1);
        if(paired) command.add(bwaReadsFile2);
        command = addToCommand(command, customArgs);
        Object[] ObjectList = command.toArray();
        String[] StringArray = Arrays.copyOf(ObjectList,ObjectList.length,String[].class);
        return StringArray;
    }
}
