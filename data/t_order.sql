/*
 Navicat Premium Data Transfer

 Source Server         : persona-test-new
 Source Server Type    : MySQL
 Source Server Version : 50721
 Source Host           : 172.16.11.82
 Source Database       : persona

 Target Server Type    : MySQL
 Target Server Version : 50721
 File Encoding         : utf-8

 Date: 04/26/2020 10:15:29 AM
*/

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `t_order`
-- ----------------------------
DROP TABLE IF EXISTS `t_order`;
CREATE TABLE `t_order` (
  `id` int(11) DEFAULT NULL,
  `name` varchar(50) DEFAULT NULL,
  `time` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `t_order`
-- ----------------------------
BEGIN;
INSERT INTO `t_order` VALUES ('1001', '用券', '2019-04-01'), ('1002', '不用券', '2019-05-10'), ('1001', '不用券', '2019-05-01'), ('1003', '不用券', '2019-04-12'), ('1001', '不用券', '2019-05-11'), ('1002', '用券', '2019-05-30'), ('1001', '不用券', '2019-05-22'), ('1003', '用券', '2019-05-24');
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
