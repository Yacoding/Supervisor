create table datamonitor(
id int(8) not null primary key auto_increment,
datetime char(20) not null,
node12 int(8) not null,
node13 int(8) not null,
monitortype char(20) not null);

create table querymonitor(
id int(8) not null primary key auto_increment,
datetime char(20) not null,
total int(8) not null,
querytype char(20) not null);