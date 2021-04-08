package me.arnu.flink.demo;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterInformationRetriever;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.configuration.JobManagerOptions.TOTAL_PROCESS_MEMORY;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;

import org.apache.hadoop.fs.FSOutputSummer;

public class SubmitJobApplicationMode {
    public static void main(String[] args) {
        app(args);
    }

    private static void perJob(String[] args) {

    }

    private static void app(String[] args) {
        //flink的本地配置目录，为了得到flink的配置
//        String configurationDirectory = System.getenv("HADOOP_CONF_DIR");
        String HADOOP_CONF_DIR = "/mnt/hgfs/hadoop/etc/hadoop";
        String FLINK_HOME = "/home/hadoop/flink-1.12.2";
        String FLINK_CONF_DIR = FLINK_HOME + "/conf";

        //存放flink集群相关的jar包目录
        String hdfs = "hdfs://vm1:9000";
        String flinkLibs = hdfs + "/flink/lib";

        List<String> libDirs = new ArrayList<>();
        Path remoteLib = new Path(flinkLibs);
        libDirs.add(remoteLib.toString());

//        String HADOOP_CLASSPATH="/mnt/hgfs/hadoop/etc/hadoop:/home/hadoop/hadoop-2.10.1/share/hadoop/common/lib/*:/home/hadoop/hadoop-2.10.1/share/hadoop/common/*:/home/hadoop/hadoop-2.10.1/share/hadoop/hdfs:/home/hadoop/hadoop-2.10.1/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop-2.10.1/share/hadoop/hdfs/*:/home/hadoop/hadoop-2.10.1/share/hadoop/yarn:/home/hadoop/hadoop-2.10.1/share/hadoop/yarn/lib/*:/home/hadoop/hadoop-2.10.1/share/hadoop/yarn/*:/home/hadoop/hadoop-2.10.1/share/hadoop/mapreduce/lib/*:/home/hadoop/hadoop-2.10.1/share/hadoop/mapreduce/*:/home/hadoop/hadoop-2.10.1//contrib/capacity-scheduler/*.jar";
//        String[] libPaths = HADOOP_CLASSPATH.split(":");
//        for (int i = 0; i < libPaths.length; i++) {
//            libPaths[i] = "file://" + libPaths[i];
//        }

//        for (String libPath : libPaths) {
//            Path path = new Path(libPath);
//            libDirs.add(path.toString());
//        }


//        String flinkUserJars = hdfs + "/flink/userjars";
        String flinkUserJars = "/mnt/hgfs/code/test/flink/target";
        //用户jar
        String userJarPath = flinkUserJars + "/flink-0.1-SNAPSHOT.jar";
        String mainClass = "me.arnu.flink.demo.FlinkSqlFromConfDemo";
//        String mainClass = "me.arnu.flink.demo.FlinkSqlDemo";

//        String mainClass = "me.arnu.flink.demo.Main";
        String flinkDistJar = flinkLibs + "/flink-dist_2.11-1.12.2.jar";

        YarnClient yarnClient = YarnClient.createYarnClient();
        org.apache.hadoop.conf.Configuration entries = new org.apache.hadoop.conf.Configuration();
        entries.addResource(new Path(HADOOP_CONF_DIR + "/yarn-site.xml"));
        entries.addResource(new Path(HADOOP_CONF_DIR + "/hdfs-site.xml"));
        entries.addResource(new Path(HADOOP_CONF_DIR + "/core-site.xml"));
        YarnConfiguration yarnConfiguration = new YarnConfiguration(entries);
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        YarnClusterInformationRetriever clusterInformationRetriever = YarnClientYarnClusterInformationRetriever
                .create(yarnClient);

        //获取flink的配置
        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(
                FLINK_CONF_DIR);
        flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        flinkConfiguration.set(
                PipelineOptions.JARS,
                Collections.singletonList(
                        userJarPath));
        flinkConfiguration.set(
                YarnConfigOptions.PROVIDED_LIB_DIRS,
                libDirs);
//                Collections.singletonList(remoteLib.toString()));

        flinkConfiguration.set(
                YarnConfigOptions.FLINK_DIST_JAR,
                flinkDistJar);
        //设置为application模式
        flinkConfiguration.set(
                DeploymentOptions.TARGET,
                YarnDeploymentTarget.APPLICATION.getName());
        //yarn application name
        flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME, "TestApplication");

        flinkConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES));
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES));

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();


        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                flinkConfiguration,
                yarnConfiguration,
                yarnClient,
                clusterInformationRetriever,
                true);


//            try {
//                yarnClusterDescriptor.deployJobCluster(clusterSpecification, new JobGraph(), false);
//            } catch (ClusterDeploymentException e) {
//                e.printStackTrace();


//    设置用户jar的参数和主类
        ApplicationConfiguration appConfig = new ApplicationConfiguration(args, mainClass);

        ClusterClientProvider<ApplicationId> clusterClientProvider = null;

        try {
            clusterClientProvider = yarnClusterDescriptor.deployApplicationCluster(
                    clusterSpecification,
                    appConfig);
        } catch (ClusterDeploymentException e) {
            e.printStackTrace();
        }

        ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();
        ApplicationId applicationId = clusterClient.getClusterId();
        System.out.println(applicationId);
    }
}