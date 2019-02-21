select column_name from information_schema.columns where table_name='t1';
select column_name from information_schema.columns where table_name='t2';
select column_name from information_schema.columns where table_name='t3';
select column_name from information_schema.columns where table_name='t4';
select column_name from information_schema.columns where table_name='t5';
select count(*) from T1
	where ( K11 is null ); 
select count(*) from T1
	group by K11 having count(*) >1; 
select count(*) from T2
	where ( K21 is null  or K22 is null ); 
select count(*) from T2
	group by K21,K22 having count(*) >1; 
select count(*) from T3
	where ( K31 is null ); 
select count(*) from T3
	group by K31 having count(*) >1; 
select count(*) from T4
	where ( K41 is null  or K42 is null ); 
select count(*) from T4
	group by K41,K42 having count(*) >1; 
select count(*) from T5
	where ( K51 is null  or K52 is null  or K53 is null ); 
select count(*) from T5
	group by K51,K52,K53 having count(*) >1; 
select count(*) from T3
	left join T1 on T3.K11=T1.K11
	where T1.K11 is null; 
select count(*) from T3
	where K11 in ( 
		select K11 from T1 group by K11 having count(*)>1); 
select count(*) from T4
	left join T3 on T4.K31=T3.K31
	where T3.K31 is null; 
select count(*) from T4
	where K31 in ( 
		select K31 from T3 group by K31 having count(*)>1); 
drop table QM;
create table QM(tableName varchar(50),entityError float(4), referentialError float(4), OK varchar(1) );
insert into QM values ('T1',0.0,0.0,'Y');
insert into QM values ('T2',30.0,0.0,'N');
insert into QM values ('T3',13.3,0.0,'N');
insert into QM values ('T4',16.6,50.0,'N');
insert into QM values ('T5',40.0,0.0,'N');
