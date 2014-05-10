drop table page;
create table page(
page_no bigserial,
file	varchar,
page	bigint,
pic_url	varchar,
pic_width	bigint,
pic_height	bigint,
reverse	int,
id	bigint,
tables_api_url	varchar,
from_txndt timestamp,
to_txndt timestamp,
txn_code varchar
)
