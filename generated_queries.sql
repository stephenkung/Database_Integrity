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
select count(*) from T4
        left join T3 on T4.K31=T3.K31
        where T3.K31 is null;
