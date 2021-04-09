# Author: Arnu
# Date: 2021-04-08 18:32:01
# Generator: MySQL-Front 5.3  (Build 4.234)

/*!40101 SET NAMES utf8 */;

#
# Structure for table "ddl_type"
#

DROP TABLE IF EXISTS `ddl_type`;
CREATE TABLE `ddl_type` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

#
# Data for table "ddl_type"
#

INSERT INTO `ddl_type` VALUES (1,'source_ddl'),(2,'sink_ddl');

#
# Structure for table "flink_job"
#

DROP TABLE IF EXISTS `flink_job`;
CREATE TABLE `flink_job` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `state` int(11) NOT NULL DEFAULT '0',
  `create_time` datetime DEFAULT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

#
# Data for table "flink_job"
#

INSERT INTO `flink_job` VALUES (1,'计算用户点赞',0,NULL,'2021-04-07 09:57:22'),(2,'用户关注圈子计算',0,'1899-12-29 00:00:00','2021-04-08 15:11:46');

#
# Structure for table "flink_ddl"
#

DROP TABLE IF EXISTS `flink_ddl`;
CREATE TABLE `flink_ddl` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `value` text,
  `type` int(11) NOT NULL DEFAULT '0',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `valid` int(11) NOT NULL DEFAULT '1',
  PRIMARY KEY (`Id`),
  KEY `ddl类型` (`type`),
  KEY `任务id` (`job_id`),
  CONSTRAINT `ddl类型` FOREIGN KEY (`type`) REFERENCES `ddl_type` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `任务id` FOREIGN KEY (`job_id`) REFERENCES `flink_job` (`Id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

#
# Data for table "flink_ddl"
#

INSERT INTO `flink_ddl` VALUES (4,1,'user_like_increase','CREATE TABLE user_like_increase (\r\n  ftime STRING,\r\n  sender_id STRING,\r\n  sender_type STRING,\r\n  receiver_id STRING,\r\n  object_id  STRING,\r\n  update_time bigint,\r\n  env STRING,\r\n  imp_date  bigint\r\n) WITH (\r\n  \'connector.type\' = \'kafka\',       \r\n\r\n  \'connector.version\' = \'universal\',\r\n  \'connector.topic\' = \'data_center-cctv_dw_flink.boss9052_test\', \r\n\r\n  \'connector.properties.bootstrap.servers\' = \'vm1:9091,vm2:9092,vm3:9092\',\r\n  \'connector.properties.group.id\' = \'flink-sql-group-1\',\r\n  \'connector.startup-mode\' = \'group-offsets\',\r\n  \'format.type\' = \'csv\'\r\n)',1,'2021-04-07 10:03:27','2021-04-07 12:05:23',1),(5,1,'receive_like','CREATE TABLE receive_like (\r\n  `user_id` varchar(30) NOT NULL,\r\n  `receive_like_cnt` BIGINT ,\r\n  `update_time` BIGINT \r\n) WITH (\r\n  \'connector.type\' = \'jdbc\',\r\n  \'connector.url\' = \'jdbc:mysql://192.168.174.1:3306/springbootdb\',\r\n  \'connector.table\' = \'receive_like\',\r\n  \'connector.driver\' = \'com.mysql.jdbc.Driver\',\r\n  \'connector.username\' = \'spring\',\r\n  \'connector.password\' = \'springboot\',\r\n  \'connector.write.flush.max-rows\' = \'1000\',\r\n  \'connector.write.flush.interval\' = \'20s\',\r\n  \'connector.write.max-retries\' = \'3\'\r\n)',2,'2021-04-07 10:12:39','2021-04-07 19:40:18',1),(6,2,'object_forward','CREATE TABLE object_forward (\r\n  ftime STRING,\r\n  vuserid STRING,\r\n  object_id STRING,\r\n  forward_to STRING,\r\n  guid  STRING,\r\n  omgid STRING,\r\n  update_time STRING,\r\n  env STRING,\r\n  os STRING\r\n) WITH (\r\n  \'connector.type\' = \'kafka\',       \r\n\r\n  \'connector.version\' = \'universal\',\r\n  \'connector.topic\' = \'data_center-cctv_dw_flink.boss9053_test\', \r\n\r\n  \'connector.properties.bootstrap.servers\' = \'vm1:9091,vm2:9092,vm3:9092\',\r\n  \'connector.properties.group.id\' = \'flink-sql-group-1\',\r\n  \'connector.startup-mode\' = \'group-offsets\',\r\n  \'format.type\' = \'csv\'\r\n)',1,'2021-04-08 15:50:58','2021-04-08 16:40:37',1),(7,2,'article_forward','CREATE TABLE article_forward (\r\n  `object_id` varchar(30) NOT NULL,\r\n  `forward_count` BIGINT ,\r\n  `update_time` BIGINT \r\n) WITH (\r\n  \'connector.type\' = \'jdbc\',\r\n  \'connector.url\' = \'jdbc:mysql://192.168.174.1:3306/springbootdb\',\r\n  \'connector.table\' = \'article_forward\',\r\n  \'connector.driver\' = \'com.mysql.jdbc.Driver\',\r\n  \'connector.username\' = \'spring\',\r\n  \'connector.password\' = \'springboot\',\r\n  \'connector.write.flush.max-rows\' = \'1000\',\r\n  \'connector.write.flush.interval\' = \'20s\',\r\n  \'connector.write.max-retries\' = \'3\'\r\n)',2,'2021-04-08 15:53:18','2021-04-08 15:53:18',1);

#
# Structure for table "flink_query"
#

DROP TABLE IF EXISTS `flink_query`;
CREATE TABLE `flink_query` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) DEFAULT '0',
  `index` int(11) DEFAULT NULL COMMENT '越大越靠前？',
  `value` text,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `valid` int(11) DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

#
# Data for table "flink_query"
#

INSERT INTO `flink_query` VALUES (1,1,0,'select receiver_id as user_id, count(sender_id) as receive_like_cnt, cast(now() as bigint) as update_time from user_like_increase group by receiver_id ','2021-04-07 10:09:48','2021-04-07 12:18:24',1),(2,2,0,'select object_id , count(1) as  forward_count, cast(now() as bigint) as update_time from object_forward group by object_id','2021-04-08 16:16:00','2021-04-08 16:16:03',1);

#
# Structure for table "job_state"
#

DROP TABLE IF EXISTS `job_state`;
CREATE TABLE `job_state` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

#
# Data for table "job_state"
#

INSERT INTO `job_state` VALUES (0,'已创建'),(1,'提交'),(2,'删除');
