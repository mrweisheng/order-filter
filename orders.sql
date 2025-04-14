/*
 Navicat Premium Dump SQL

 Source Server         : wenzhi乌班图
 Source Server Type    : MySQL
 Source Server Version : 80041 (8.0.41-0ubuntu0.20.04.1)
 Source Host           : 118.31.76.202:3306
 Source Schema         : wenzhi

 Target Server Type    : MySQL
 Target Server Version : 80041 (8.0.41-0ubuntu0.20.04.1)
 File Encoding         : 65001

 Date: 13/04/2025 11:22:04
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for orders
-- ----------------------------
DROP TABLE IF EXISTS `orders`;
CREATE TABLE `orders`  (
  `order_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '订单编号',
  `payment_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '支付单号',
  `amount` decimal(10, 2) NOT NULL DEFAULT 0.00 COMMENT '买家实付金额',
  `status` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '订单状态',
  `create_time` datetime NOT NULL COMMENT '订单创建时间',
  `merchant_remark` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '商家备注',
  `refund_amount` decimal(10, 2) NOT NULL DEFAULT 0.00 COMMENT '卖家实退金额',
  `fee` decimal(10, 2) NOT NULL DEFAULT 0.00 COMMENT '手续费',
  `channel` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '渠道（支付宝/企业微信）',
  `confirm_time` datetime NULL DEFAULT NULL COMMENT '确认收货时间',
  `merchant_payment` decimal(10, 2) NULL DEFAULT NULL COMMENT '打款商家金额',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  `customer_id` int NULL DEFAULT NULL COMMENT '订单对应客服',
  `writer_id` int NULL DEFAULT NULL COMMENT '订单对应写手',
  PRIMARY KEY (`order_id`) USING BTREE,
  INDEX `idx_payment_id`(`payment_id` ASC) USING BTREE,
  INDEX `idx_create_time`(`create_time` ASC) USING BTREE,
  INDEX `idx_channel`(`channel` ASC) USING BTREE,
  INDEX `idx_status`(`status` ASC) USING BTREE,
  INDEX `fk_customer`(`customer_id` ASC) USING BTREE,
  INDEX `fk_writer`(`writer_id` ASC) USING BTREE,
  CONSTRAINT `fk_customer` FOREIGN KEY (`customer_id`) REFERENCES `users` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_writer` FOREIGN KEY (`writer_id`) REFERENCES `writer_info` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '订单数据表' ROW_FORMAT = DYNAMIC;

SET FOREIGN_KEY_CHECKS = 1;
