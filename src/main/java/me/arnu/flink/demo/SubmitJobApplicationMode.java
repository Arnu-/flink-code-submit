package me.arnu.flink.demo;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.client.JobStatusMessage;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

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
        String[] flinkArgs = new String[0];
        String hdfs = "hdfs://vm1:9000";
        // 从参数中取出目标jar包，目标主类，以及对应参数
        String flinkUserJars = hdfs + "/flink/userjars";
        // String flinkUserJars = "/mnt/hgfs/code/test/flink/target";
        //用户jar
        String userJarPath = flinkUserJars + "/flink-0.1-SNAPSHOT.jar";
        String mainClass = "me.arnu.flink.demo.FlinkSqlFromConfDemo";

        if (args != null && args.length > 0) {
            if (args.length == 1) {
                flinkArgs = args;
            } else if (args.length == 2) {
                mainClass = args[0];
                flinkArgs = new String[]{args[1]};
            } else {
                userJarPath = args[0];
                mainClass = args[1];
                flinkArgs = Arrays.copyOfRange(args, 2, args.length);
            }
        }
//        String mainClass = "me.arnu.flink.demo.FlinkSqlDemo";

//        String mainClass = "me.arnu.flink.demo.Main";


        //flink的本地配置目录，为了得到flink的配置
//        String configurationDirectory = System.getenv("HADOOP_CONF_DIR");
        String HADOOP_CONF_DIR = "/mnt/hgfs/hadoop/etc/hadoop";
        String FLINK_HOME = "/home/hadoop/flink-1.12.2";
        String FLINK_CONF_DIR = FLINK_HOME + "/conf";

        //存放flink集群相关的jar包目录
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


        String flinkDistJar = flinkLibs + "/flink-dist_2.11-1.12.2.jar";

        // 创建一个 yarn client，这里需要获取yarn的各项配置信息，主要用于与yarn进行通信
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
        // 设置启用增量Checkpoint
        flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        // 指定要提交的Flink任务的 jar 包
        flinkConfiguration.set(
                PipelineOptions.JARS,
                Collections.singletonList(
                        userJarPath));

        // 指定依赖的目录，在 yarn application mode 下，可以指定 hdfs路径作为依赖路径
        flinkConfiguration.set(
                YarnConfigOptions.PROVIDED_LIB_DIRS,
                libDirs);

        // 指定 FLINK_DIST_JAR 路径，这里可以指定hdfs上的路径
        flinkConfiguration.set(
                YarnConfigOptions.FLINK_DIST_JAR,
                flinkDistJar);

        //设置为application模式
        flinkConfiguration.set(
                DeploymentOptions.TARGET,
                YarnDeploymentTarget.APPLICATION.getName());

        //设定在 yarn 显示的 application name
        flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME, "TestApplication");

        // 设定jm和tm的内存大小
        flinkConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES));
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES));

        // 定义一个用于指定集群资源的实例
        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();

        // 初始化基于 yarn 的 cluster descriptor 实例。
        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                flinkConfiguration,
                yarnConfiguration,
                yarnClient,
                clusterInformationRetriever,
                true);

// per-job的提交方式【已弃用】
//            try {
//                yarnClusterDescriptor.deployJobCluster(clusterSpecification, new JobGraph(), false);
//            } catch (ClusterDeploymentException e) {
//                e.printStackTrace();


//    设置用户jar的参数和主类
        ApplicationConfiguration appConfig = new ApplicationConfiguration(flinkArgs, mainClass);

        ClusterClientProvider<ApplicationId> clusterClientProvider = null;

        try {
            // 部署集群
            clusterClientProvider = yarnClusterDescriptor.deployApplicationCluster(
                    clusterSpecification,
                    appConfig);
        } catch (ClusterDeploymentException e) {
            e.printStackTrace();
        }

        ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();
        ApplicationId applicationId = clusterClient.getClusterId();
        System.out.println(applicationId);

        try {
            // 通过 client 来获取任务状态
            for (int i = 0; i < 20; i++) {
                CompletableFuture<Collection<JobStatusMessage>> jobs = clusterClient.listJobs();
                jobs.whenComplete((j, e) -> {
                    for (JobStatusMessage s : j) {
                        System.out.printf("start: %s, jobId: %s, jobName: %s, jobState: %s%n"
                                , s.getStartTime(), s.getJobId(), s.getJobName(), s.getJobState());
                    }
                });
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        try {
            // 用来使程序等待
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}