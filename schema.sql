CREATE DATABASE IF NOT EXISTS `xlstest`

CREATE TABLE IF NOT EXISTS `logs` (
    `id` int(11) NOT NULL auto_increment, 
    `page_url` varchar(1024)  NOT NULL,
    `product_name` varchar(255)  NOT NULL,
    `appid` varchar(255) NOT NULL,
    `proto` varchar(64) NOT NULL DEFAULT '',
    `copyright` varchar(255) NOT NULL DEFAULT '',
    `build_version` varchar(64) NOT NULL DEFAULT '',
    `author` varchar(64) NOT NULL DEFAULT '',
    `user_id` int(11) NOT NULL,
    `created_at` TIMESTAMP NOT NULL DEFAULT NOW(),
    `updated_at` TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(),
    PRIMARY KEY(`id`)
);
