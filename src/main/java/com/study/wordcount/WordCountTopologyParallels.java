package com.study.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopologyParallels {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) {
        SentenceSpout spout = new SentenceSpout();
        SplitBolt splitBolt = new SplitBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2); // Spout 연결
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID); // Spout -> SplitBolt
        builder.setBolt(COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word")); // SplitBolt -> CountBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID); // CountBolt -> ReportBolt

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setNumWorkers(2);

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        try { Thread.sleep(1000 * 10); } catch (InterruptedException e) { } // waiting 10s
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}