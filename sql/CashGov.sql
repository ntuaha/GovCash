drop table GovCash;
create table GovCash (
No	Bigserial,
page	bigint,
row	bigint,
txn_dt	timestamp,
txn_code	varchar,
user_nm	varchar,
id	varchar,
receive_money	bigint,
pay_money	bigint,
cash_ind	varchar,
area	varchar,
correct_rate real,
correct_rate_imp real

)


