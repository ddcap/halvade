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

package be.ugent.intec.halvade.tools;

import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import be.ugent.intec.halvade.utils.CommandGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import java.text.DecimalFormat;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ddecap
 */
public class GATKTools {
    protected static final String DISABLE_VCF_LOCKING = "--disable_auto_index_creation_and_locking_when_reading_rods";
    protected static final String NO_CMD_HEADER = "--no_cmdline_in_header";
    // params
    String reference;
    String bin;
    String gatk;
    String java;
    String mem = "-Xmx2g";
    int threadingType = 0; // [default] 0 = data multithreading (1 = cpu multithreading)
    int threads = 1; 
    String[] multiThreadingTypes = {"-nt", "-nct"};
    DecimalFormat onedec;
    Reducer.Context context;
    
    public void setThreads(int threads) {
        this.threads = threads;
    }
    
    public void setThreadingType(int threadingType) {
        this.threadingType = threadingType % 2;
    }

    public void setContext(Reducer.Context context) {
        this.context = context;
        mem = "-Xmx" + context.getConfiguration().get("mapreduce.reduce.memory.mb") + "m";
    }
    
    public void setMemory(int megs) {
        mem = "-Xmx" + megs + "m";
    }
        
    public GATKTools(String reference, String bin) {
        this.reference = reference;
        this.bin = bin;
        this.java = "java";
        this.gatk = bin + "/GenomeAnalysisTK.jar" ;
        onedec = new DecimalFormat("###0.0");
    }

    public String getJava() {
        return java;
    }

    public void setJava(String java) {
        this.java = java;
    }  
    
    public String roundOneDecimal(double val) {
        return onedec.format(val);
    }
    
    private static String[] AddCustomArguments(String[] command, String customArgs) {
        if(customArgs == null || customArgs.isEmpty()) return command;
        ArrayList<String> tmp = new ArrayList(Arrays.asList(command));
        tmp = CommandGenerator.addToCommand(tmp, customArgs);  
        Object[] ObjectList = tmp.toArray();
        return Arrays.copyOf(ObjectList,ObjectList.length,String[].class);  
    }
    
    public void runBaseRecalibrator(String input, String table, String ref, String knownSite, String region) throws InterruptedException {
        String[] knownSites = {knownSite};
        runBaseRecalibrator(input, table, ref, knownSites, region);        
    }
            
    public void runBaseRecalibrator(String input, String table, String ref, String[] knownSites, String region) throws InterruptedException {        
        /**
         * example: from CountCovariates
         * -I input.bam -T Countcovariates -R ref -knownSites dbsnp
         * -cov ReadGroupCovariate -cov QualityScoreCovariate -cov DinucCovariate
         * -cov HomopolymerCovariate
         * -recalFile recal.csv
         * 
         * java -Xmx4g -jar GenomeAnalysisTK.jar \
            -T BaseRecalibrator \
            -I my_reads.bam \
            -R resources/Homo_sapiens_assembly18.fasta \
            -knownSites bundle/hg18/dbsnp_132.hg18.vcf \
            -knownSites another/optional/setOfSitesToMask.vcf \
            -o recal_data.table
         */

        ArrayList<String> command = new ArrayList<>();
        String[] gatkcmd = {
            java, mem, "-jar", gatk,
            "-T", "BaseRecalibrator",
            multiThreadingTypes[1], "" + threads, // only -nct
            "-R", ref,
            "-I", input,
            "-o", table,
            "-L", region,
            DISABLE_VCF_LOCKING};
        command.addAll(Arrays.asList(gatkcmd));
        for(String knownSite : knownSites) {
            command.add("-knownSites");
            command.add(knownSite);
        }
//        command.addAll(Arrays.asList(covString));
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "baserecalibrator");
        command = CommandGenerator.addToCommand(command, customArgs);   
        Object[] objectList = command.toArray();
        long estimatedTime = runProcessAndWait("GATK BaseRecalibrator", Arrays.copyOf(objectList,objectList.length,String[].class));
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_RECAL).increment(estimatedTime);
    }

    public void runRealignerTargetCreator(String input, String targets, String ref, String region) throws InterruptedException {
        /**
         * example: 
         * java -Xmx2g -jar GenomeAnalysisTK.jar \
         * -T RealignerTargetCreator \
         * -R ref.fasta \
         * -I input.bam \
         * -o forIndelRealigner.intervals  
         * 
         */
        String[] command = {
            java, mem, "-jar", gatk,
            "-T", "RealignerTargetCreator",
            multiThreadingTypes[0], "" + threads, // only supports -nt
            "-R", ref,
            "-I", input,
            "-o", targets,
            "-L", region};
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "realignertargetcreator");
        long estimatedTime = runProcessAndWait("GATK RealignerTargetCreator", AddCustomArguments(command, customArgs));    
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_TARGET_CREATOR).increment(estimatedTime);
    }

    public void runSplitNCigarReads(String input, String output, String ref, String region, int newMaxQualScore) throws InterruptedException {
        /**
         * example: 
         * java -Xmx4g -jar GenomeAnalysisTK.jar 
         * -T SplitNCigarReads 
         * -R ref.fasta 
         * -I dedupped.bam 
         * -o split.bam 
         * -rf ReassignOneMappingQuality 
         * -RMQF 255 
         * -RMQT 60 
         * -U ALLOW_N_CIGAR_READS  
         * 
         */
        String[] command = {
            java, mem, "-jar", gatk,
            "-T", "SplitNCigarReads",
            "-R", ref,
            "-I", input,
            "-o", output,
            "-rf", "ReassignOneMappingQuality",
            "-RMQF", "255",
            "-RMQT", "" + newMaxQualScore,
            "-U", "ALLOW_N_CIGAR_READS",
            "-L", region};
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "splitncigarreads");
        long estimatedTime = runProcessAndWait("GATK SplitNCigarReads", AddCustomArguments(command, customArgs));   
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_INDEL_REALN).increment(estimatedTime);   
        
    }
    
    public void runVariantFiltration(String input, String output, String ref, String region, int window, int cluster, double minFS, double maxQD) throws InterruptedException {
        /**
         * example: 
         * java -Xmx4g -jar GenomeAnalysisTK.jar 
         * -T VariantFiltration 
         * -R hg_19.fasta 
         * -V input.vcf 
         * -o output.vcf 
         * -window 35 
         * -cluster 3 
         * -filterName FS 
         * -filter "FS > 30.0" 
         * -filterName QD 
         * -filter "QD < 2.0" 
         * 
         */
        String[] command = {
            java, mem, "-jar", gatk,
            "-T", "VariantFiltration",
            "-R", ref,
            "-V", input,
            "-o", output,
            "-L", region,
            "-window", "" + window,
            "-cluster", "" + cluster,
            "-filterName", "FS",
            "-filter", "FS > " + roundOneDecimal(minFS),
            "-filterName", "QD",
            "-filter", "QD < " + roundOneDecimal(maxQD)};
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "variantfiltration");
        long estimatedTime = runProcessAndWait("GATK VariantFiltration", AddCustomArguments(command, customArgs));   
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_INDEL_REALN).increment(estimatedTime);   
        
    } 
    
    public void runVariantAnnotator(String input, String output, String ref, String region) throws InterruptedException {
        /**
         * example: 
         * java -Xmx4g -jar GenomeAnalysisTK.jar 
         * -R ref.fasta \
         * -T VariantAnnotator \
         * -I input.bam \
         * -o output.vcf \
         * 
         * -A Coverage \
         * --variant input.vcf \
         * -L input.vcf \
         * --dbsnp dbsnp.vcf
         * 
         */
        String[] command = {
            java, mem, "-jar", gatk,
            "-T", "VariantAnnotator",
            multiThreadingTypes[0], "" + threads, 
            "-R", ref,
            "-V", input,
            "-o", output,
            "-L", region};
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "variantannotator");
        long estimatedTime = runProcessAndWait("GATK VariantAnnotator", AddCustomArguments(command, customArgs));   
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_INDEL_REALN).increment(estimatedTime);
    } 

    public void runIndelRealigner(String input, String targets, String output, String ref, String region) throws InterruptedException {
        /**
         * example: 
         * java -Xmx4g -jar GenomeAnalysisTK.jar \
         * -T IndelRealigner \
         * -R ref.fasta \
         * -I input.bam \
         * -targetIntervals intervalListFromRTC.intervals \
         * -o realignedBam.bam \
         * [-known /path/to/indels.vcf] \
         * [-compress 0]    
         * 
         */
        String[] command = {
            java, mem, "-jar", gatk,
            "-T", "IndelRealigner",
            "-R", ref,
            "-I", input,
            "-targetIntervals", targets,
            "-o", output,
            "-L", region};
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "indelrealigner");
        long estimatedTime = runProcessAndWait("GATK IndelRealigner", AddCustomArguments(command, customArgs));   
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_INDEL_REALN).increment(estimatedTime);    
    }

    public void runPrintReads(String input, String output, String ref, String table, String region) throws InterruptedException {
        /**
         * example:
         * -I input.bam -o recalibrated.bam -T TableRecalibration -recalFile recal.csv -R ref
         ***************************************************************************
         * Not using multi-threading, tests show best performance is single thread *
         ***************************************************************************
         */
        String[] command = {
            java, mem, "-jar", gatk,
            "-T", "PrintReads",
            "-R", ref,
            "-I", input,
            "-o", output,
            "-BQSR", table,
            "-L", region};
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "printreads");
        long estimatedTime = runProcessAndWait("GATK PrintReads", AddCustomArguments(command, customArgs));  
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_PRINT_READS).increment(estimatedTime);        
    }

    public void runCombineVariants(String[] inputs, String output, String ref) throws InterruptedException {
        /**
         *  java -Xmx2g -jar GenomeAnalysisTK.jar \
         *  -R ref.fasta \
         *  -T CombineVariants \
         *  --variant input1.vcf \
         *  --variant input2.vcf \
         *  -o output.vcf \
         *  -genotypeMergeOptions UNIQUIFY
         */
        ArrayList<String> command = new ArrayList<String>();
        
        String[] gatkcmd = {
            java, mem, "-jar", gatk,
            "-T", "CombineVariants",
            multiThreadingTypes[0], "" + threads,
            "-R", ref,
            "-o", output, "-sites_only",
            "-genotypeMergeOptions", "UNIQUIFY"};
        command.addAll(Arrays.asList(gatkcmd));
        if(inputs != null) {
            for(String input : inputs) {
                command.add("--variant");
                command.add(input);
            }
        }
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "combinevariants");
        command = CommandGenerator.addToCommand(command, customArgs);   
        Object[] objectList = command.toArray();
        long estimatedTime = runProcessAndWait("GATK CombineVariants", Arrays.copyOf(objectList,objectList.length,String[].class));
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_COMBINE_VCF).increment(estimatedTime);
    }

    public void runUnifiedGenotyper(String input, String output, double scc, double sec, String ref, 
            String[] knownSites, String region) throws InterruptedException {
        /**
         * example:
         * -I recalibrated.bam -T UnifiedGenotyper -o output.vcf -R ref
         */
        ArrayList<String> command = new ArrayList<String>();            
        
        String[] gatkcmd = {
            java, mem, "-jar", gatk,
            "-T", "UnifiedGenotyper",
            multiThreadingTypes[threadingType], "" + threads, 
            "-R", ref,
            "-I", input,
            "-o", output,
            "-stand_call_conf", roundOneDecimal(scc),
            "-stand_emit_conf", roundOneDecimal(sec),
            "-L", region,
            NO_CMD_HEADER,
            DISABLE_VCF_LOCKING};
        command.addAll(Arrays.asList(gatkcmd));
        if(knownSites != null) {
            for(String knownSite : knownSites) {
                command.add("-dbsnp");
                command.add(knownSite);
            }
        }
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "variantcaller");
        command = CommandGenerator.addToCommand(command, customArgs);   
        Object[] objectList = command.toArray();
        long estimatedTime = runProcessAndWait("GATK UnifiedGenotyper", Arrays.copyOf(objectList,objectList.length,String[].class));   
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_VARIANT_CALLER).increment(estimatedTime);
    }

    public void runHaplotypeCaller(String input, String output, boolean disableSoftClipping, 
            double scc, double sec, String ref, String[] knownSites, String region) throws InterruptedException {
        /**
         * example:
         * -I recalibrated.bam -T UnifiedGenotyper -o output.vcf -R ref
         */
        ArrayList<String> command = new ArrayList<String>();
        
        String[] gatkcmd = {
            java, mem, "-jar", gatk,
            "-T", "HaplotypeCaller",
            multiThreadingTypes[1], "" + threads, 
            "-R", ref,
            "-I", input,
            "-o", output,
            "-stand_call_conf", roundOneDecimal(scc),
            "-stand_emit_conf", roundOneDecimal(sec),
            "-L", region,
            NO_CMD_HEADER,
            DISABLE_VCF_LOCKING};
        command.addAll(Arrays.asList(gatkcmd));
        if(disableSoftClipping) {
            command.add("-dontUseSoftClippedBases");
        }
        if(knownSites != null) {
            for(String knownSite : knownSites) {
                command.add("-dbsnp");
                command.add(knownSite);
            }
        }
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "gatk", "variantcaller");
        command = CommandGenerator.addToCommand(command, customArgs);   
        Object[] objectList = command.toArray();
        long estimatedTime = runProcessAndWait("GATK HaplotypeCaller", Arrays.copyOf(objectList,objectList.length,String[].class));   
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_GATK_VARIANT_CALLER).increment(estimatedTime);
    }
    
    private long runProcessAndWait(String name, String[] command) throws InterruptedException {
        long startTime = System.currentTimeMillis();
//        HalvadeHeartBeat hhb = new HalvadeHeartBeat(context);
//        hhb.start();
        ProcessBuilderWrapper builder = new ProcessBuilderWrapper(command, null);
        builder.startProcess(true);
        int error = builder.waitForCompletion();
//        hhb.jobFinished();
//        hhb.join();
        if(error != 0)
            throw new ProcessException(name, error);
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("estimated time: " + estimatedTime / 1000);
        return estimatedTime;
    }
}
