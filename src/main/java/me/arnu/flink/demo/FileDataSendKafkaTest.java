package me.arnu.flink.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 发送测试数据，并接收计算结果，判断计算结果与发送数据之间的差异
 */
public class FileDataSendKafkaTest {
    static Logger logger = LoggerFactory.getLogger(FileDataSendKafkaTest.class);
    static boolean running = false;
    static boolean fromStart = false;

    public static void main(String[] args) throws IOException {
//        StringBuilder formatStr = new StringBuilder("arnuTest [-h/--help] 测试kafka读写");
//        HelpFormatter formatter = new HelpFormatter();
//        Options opt = new Options();
//        opt.addOption("h", "help", false, "显示帮助信息");
//        opt.addOption("s", "send", true, "发送随机测试数据, 数据跟条数");
//        opt.addOption("r", "recv", false, "接收生成数据");
//        opt.addOption("b", "begin", false, "从头开始接收数据");
//        opt.addOption("h", "help", false, "显示帮助信息");
//        if (args == null || args.length == 0) {
//            formatter.printHelp(formatStr.toString(), opt);
//            return;
//        }

        if (args == null || args.length == 0) {
            args = new String[]{"send"};
        }

        Map<String, Object> prodprops = new HashMap<>();
        if (args.length >= 2) {
            if (args[1].equals("s")) {
                fromStart = true;
            }
        }
//        String bossId = args[0];
//        String indexName = args[1];
        String bossId = "9050";
        String indexName = "article_comment";
        if (args.length < 3) {
            bossId = "9050";
            indexName = "article_comment";
        } else {
            bossId = args[0];
            indexName = args[1];
        }

//        ResourceBundle kafka_rb = ResourceBundle.getBundle("kafka_scm_consumer");
//        prodprops.put("bootstrap.servers", kafka_rb.getString("consumer.bootstrap.servers"));
        prodprops.put("bootstrap.servers", "vm1:9092,vm2:9092,vm3:9092");
        prodprops.put("acks", "1");
        prodprops.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodprops.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


//        String prod_topic = kafka_rb.getString("consumer.topic_" + bossId);
        String prod_topic ="";

        int count = 10000;
        int arMax = 50;

        Thread sendT = new Thread(new SendToKafka(arMax, count, prod_topic, prodprops));

        running = true;
        if (args[0].equals("send") || args[0].
                equals("both")) {
            sendT.start();
        }

        running = false;
        System.in.read();

    }


    /**
     * 发送数据到kafka
     */
    public static class SendToKafka implements Runnable {
        private int arIdMax = 0;

        private int count = 0;
        private String broker = "";
        private String topic = "";
        private Map<String, Object> conf;

        SendToKafka(int arIdMax, int count, String topic, Map<String, Object> conf) {
            this.arIdMax = arIdMax;
            this.count = count;
            this.topic = topic;
            this.conf = conf;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() {
            logger.info("启动发送kafka");
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);

            /*
            20日：0
            21日：222727
            22日：387019
            23日：490673
             */

            /*long day20 = 0L;
            long day21 = 222727L;
            long day22 = 387019L;
            long day23 = 490673L;*/

            /*//9050
            long day20 = 0L;
            long day21 = 18L;
            long day22 = 27L;
            long day23 = 355L;
            String fileName = "D:\\tmp\\20210324\\scm_user_join_circle.csv";
            ResourceBundle kafka_rb = ResourceBundle.getBundle("kafka_scm_consumer");
            topic = kafka_rb.getString("consumer.topic_" + "9050");*/

            // 9051
            /*long day20 = 0L;
            long day21 = 120285L;
            long day22 = 0;
            long day23 = 332812L;
           // String fileName = "D:\\tmp\\20210324\\scm_user_follow_user_322.csv";
            String fileName = "D:\\tmp\\20210324\\scm_user_follow_user.csv";
//            ResourceBundle kafka_rb = ResourceBundle.getBundle("kafka_scm_consumer");
//            topic = kafka_rb.getString("consumer.topic_" + "9051");
            topic = "vm-topic-1";*/

            // 9052
            /*long day20 = 0L;
            long day21 = 5L;
            long day22 = 7L;
            long day23 = 149L;
            String fileName = "D:\\tmp\\20210324\\scm_user_like_increase.csv";
//            ResourceBundle kafka_rb = ResourceBundle.getBundle("kafka_scm_consumer");
            topic = "data_center-cctv_dw_flink.boss9052_test";
//            topic = "vm-topic-1";*/


            // 9053
//            topic = "vm-topic-1";
            long day20 = 0L;
            long day21 = 5L;
            long day22 = 0L;
            long day23 = 1490L;
            String fileName = "D:\\tmp\\20210324\\scm_object_forward_322.csv";
//            ResourceBundle kafka_rb = ResourceBundle.getBundle("kafka_scm_consumer");
            topic = "data_center-cctv_dw_flink.boss9053_test";

            /*long day20 = 0L;
            long day21 = 1L;
            long day22 = 2L;
            long day23 = 160L;*/

            // 9054
            /*long day20 = 0L;
            long day21 = 6L;
            long day22 = 8L;
            long day23 = 323L;
            String fileName = "D:\\tmp\\20210324\\scm_object_like_322.csv";
//            ResourceBundle kafka_rb = ResourceBundle.getBundle("kafka_scm_consumer");
            topic = "data_center-cctv_dw_flink.boss9054_test";*/

            try {
                TextFileReader.yield(fileName
                        , 1, 1, 200
//                        , day21
                        , day22
                        , day23
//                        , Integer.MAX_VALUE
                        , r -> {
                            r = r.replace("\t", ",");
                            String timestamp = String.valueOf(System.currentTimeMillis());
                            Future<RecordMetadata> f = producer.send(new ProducerRecord<>(topic, timestamp, r));
                            try {
                                f.get();
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                            return null;
                        });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private static int random(int max, int min) {

        return (int) (Math.random() * (max - min) + min);
    }

    private static class TextFileReader {


        public static void yield(String fileName, int showStep, int batch, int sleep
                , long start, long end
                , Function<String, Void> apply) throws InterruptedException {
            Scanner inputStream = null;
            try {
                inputStream = new Scanner(new FileInputStream(fileName));
            } catch (FileNotFoundException e) {
                System.out.println("File " + fileName + " was no found");
                System.exit(0);
            }
            String line = null;
            long count = 0;
            long to = 0;
            long oneBatch = 0;
            long cursor = 0;
            while (inputStream.hasNextLine() && cursor < end - 1) {
                if (cursor < start) {
                    line = inputStream.nextLine();
                    cursor++;
                    continue;
                }
                cursor++;
                count++;
                long now = count / showStep;
                if (now > to) {
                    to = now;
                    System.out.print(count + ",");
                }
                line = inputStream.nextLine();
                apply.apply(line);
                oneBatch++;
                if (oneBatch >= batch) {
                    if (sleep > 0) {
                        TimeUnit.MILLISECONDS.sleep(sleep);
                    }
                    oneBatch = 0;
                }
            }
            inputStream.close();
            System.out.println("file End.");
        }
    }
}
