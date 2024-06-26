CREATE SCHEMA IF NOT EXISTS testdb;
SET SCHEMA testdb;

CREATE TABLE IF NOT EXISTS trade_tbl (
   id INT NOT NULL,
   tradeId INT NOT NULL,
   account VARCHAR(20) NOT NULL,
   securityId VARCHAR(10) NOT NULL,
   quantity INT NOT NULL
);