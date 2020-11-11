DROP TABLE IF EXISTS `tbl_phone_lib_md5`;
CREATE TABLE `tbl_phone_lib_md5` (
  Id BIGINT NOT NULL AUTO_INCREMENT,
  PType INT NOT NULL COMMENT '',
  PhoneNumber BIGINT NOT NULL COMMENT '',
  PhoneMD5 varchar(255) NOT NULL DEFAULT '',
  INDEX(PhoneMD5),
  PRIMARY KEY (Id)
) DEFAULT charset = utf8;
