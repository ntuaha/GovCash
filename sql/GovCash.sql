
drop table GovCash;
create table GovCash (
No	Bigserial,
page	bigint,
row	bigint,
txn_dt	timestamp,
main_txn_code varchar,
txn_code	varchar,
user_nm	varchar,
id	varchar,
receive_money	bigint,
pay_money	bigint,
cash_ind	varchar,
area	varchar,
correct_rate real,
correct_rate_imp real,
candidate_no int
);
insert into govcash (page,row,txn_dt,txn_code,user_nm,id,receive_money,pay_money,cash_ind,area,correct_rate,correct_rate_imp,candidate_no)  (select  
A.page,A.row,A.txn_dt ,A.txn_code,A.user_nm,A.id,A.receive_money,A.pay_money,A.cash_ind,  
A.area, A.correct_rate,A.correct_rate_imp ,B.candidate_no from govcash_pre as A left join page as B on (A.page = B.id) order by page,row);

\copy (select candidate_no,page,row,to_char(txn_dt,'YYYY-MM-DD') as txn_dt,main_txn_code,txn_code,user_nm,id,receive_money,pay_money,cash_ind,area,to_char(correct_rate,'0D999') as correct_rate,to_char(correct_rate_imp,'0D999') as correct_rate_imp from govcash order by page,row) To '/home/aha/Project/GovCash/data/govcash.csv' With CSV HEADER




--drop table C;
--create temporary table C as select file,regexp_matches(file, '第(\d+)[屆|任](.+)擬參選人(.+)政治獻金專戶', 'g') as A from (select distinct file from page) as B ;
--update page as  A set account=C.A[3],term = C.A[1]::int,position = C.A[2] from C where A.file =C.file;
--\copy (select * from page) To '/home/aha/Project/GovCash/data/page.csv' With CSV HEADER


--select file,page,id,account,position,term,txn_code from page limit 10;