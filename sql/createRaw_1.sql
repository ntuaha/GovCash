drop table GovCash_TXN;
create table GovCash_TXN(
Txn_No	bigserial,
page	bigint,
row	bigint,
col	bigint,
ans	varchar,
user_id	bigint,
time	timestamp
);
