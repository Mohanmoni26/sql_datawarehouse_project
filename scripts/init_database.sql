For the datawarehouse project , we had been using medallion architecture, it consists of three layers Bronze, silver and gold layer where all the 
layers has characteristics, we had created three schemas for that in the Database 'Datawarehouse' named Bronze, silver and gold.


---- Create database 'DataWarehouse'
create database Datawarehouse;
go

---- Create schema 'Bronze'

create schema Bronze;
go

---- Create schema 'silver'
create schema silver;
go

---- Create schema 'gold'
create schema Gold;
