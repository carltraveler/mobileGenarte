DROP TABLE IF EXISTS `tbl_phone_lib_md5`;
CREATE TABLE `tbl_phone_lib_md5` (
  PhoneNumber BIGINT NOT NULL COMMENT '',
  PhoneMD5 varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (PhoneMD5)
) DEFAULT charset = utf8;
