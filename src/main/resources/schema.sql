DROP TABLE IF EXISTS companies;
 
CREATE TABLE companies (
  id INT AUTO_INCREMENT  PRIMARY KEY,
  name varchar(255)  NOT NULL,
  symbol varchar(16) NOT NULL,
  status VARCHAR(8) default NULL
  
);
 
insert into companies values(1, 'Bitcoin','BTC-USD','PENDING');
insert into companies values(2, 'Cardano','ADA-USD','PENDING');
insert into companies values(3, 'Ethereum','ETH-USD','PENDING');
insert into companies values(4, 'GOLD','GC=F','PENDING');