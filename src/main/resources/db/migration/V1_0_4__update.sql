ALTER TABLE `charge_box`
    ADD COLUMN `pin_code` VARCHAR(100) NULL ;

ALTER TABLE `ocpp_tag`
    ADD COLUMN `nick_name` VARCHAR(255) NULL ;