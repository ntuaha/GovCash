
drop table UserInfo;
drop table TableColumn;
drop table Votes;

-- Basic Information

create temporary table T as select distinct page,row,col from govcash_txn order by page,row,col;
create temporary table UP as select  user_id,count(*) as Input_Cnt from govcash_txn group by user_id;

-- 計算使用者資料使用
create temporary table B as select page,row,col,user_id,max(txn_no) as max_txn from govcash_txn where ans != 'FFFFFFFFFFFFF' group by page,row,col,user_id;
create temporary table B2 as select  A.* from govcash_txn as A inner join B on (A.page =B.page and A.row = B.row and A.col=B.col and  A.user_id = B.user_id and A.txn_no = B.max_txn);

-- 計算欄位資訊使用
EXPLAIN ANALYZE create temporary table BB2 as select  * from (select *,rank() over (partition by page,row,col,user_id order by txn_no desc) from govcash_txn) as A  where rank=1;
-- 驗證等價
--create temporary table CBB as select page,row,col,user_id,max(txn_no) as max_txn from govcash_txn group by page,row,col,user_id;
--create temporary table CBB2 as select  A.* from govcash_txn as A inner join CBB as B on (A.page =B.page and A.row = B.row and A.col=B.col and  A.user_id = B.user_id and A.txn_no = B.max_txn);

create temporary table B3 as select  A.page,A.row,A.col,A.user_id,coalesce(B.ans,A.ans) as ans,a.original_ans,coalesce(B.txn_no,A.txn_no) as txn_no,coalesce(B.time,A.time) as time from BB2 as A left join B2 as B on (A.page =B.page and A.row = B.row and A.col=B.col and  A.user_id = B.user_id);
drop table B,B2,BB2;
create table Votes as select * from B3;	

-- 計算答案
create temporary table A as select page,row,col,ans,count(user_id) as Cnt    from B3 where ans != 'FFFFFFFFFFFFF' group by page,row,col,ans;  
--create temporary table A2 as select page,row,col,max(cnt) as max_cnt,sum(cnt) as sum_cnt from A group by page,row,col having sum(cnt)>1;
--create temporary table A3 as select A.*  from A as A inner join A2 as B on (A.page=B.page and A.row=B.row and A.col=B.col and A.cnt = B.max_cnt)  order by page,row,col;
create temporary table A3 as select * from (select *,rank() over (partition by page,row,col order by cnt desc) from A) as A2 where rank=1;
create temporary table A4 as select page,row,col,count(*) as cnt from A3 as A group by page,row,col having count(*)=1;
create temporary table A6 as select A.* from A3 as A inner join A4 as B on (A.page=B.page and A.row=B.row and A.col=B.col);
create temporary table A7 as select distinct page,row,col from A6 ;
drop table A,A3,A4;




-- 正確欄位  錯誤欄位

create temporary table T1 as select A.page,A.row,A.col,A.user_id,A.ans,A.original_ans,A.time,case when B.page is null then 0 else 1 end as Correct_Ind,case when A.ans = 'FFFFFFFFFFFFF' then 1 else 0 end as Error_Ind
from B3 as A
left join A6 as B on  (A.page = B.page and A.row = B.row and A.col= B.col and A.ans= B.ans)
inner join A7 as C on (A.page = C.page and A.row = C.row and A.col= C.col);


-- 使用者資訊


create temporary table U1 as select user_id,count(*) as Valid_Input_Cnt from (select page,row,col,user_id from B3 where ans != 'FFFFFFFFFFFFF') as A group by user_id;
create temporary table U2 as select user_id,count(*) as Correct_Cnt from T1  where Correct_Ind=1 group by user_id;
create temporary table U3 as select user_id,count(*) as Odd_Input_Cnt  from (select page,row,col,user_id from B3 where ans = 'FFFFFFFFFFFFF') as A group by user_id;

create temporary table U4 as select 
A.*, 
coalesce(B.Valid_Input_Cnt,0) as Valid_input_Cnt,
coalesce(C.Correct_Cnt,0) as Correct_Cnt,
coalesce(D.Odd_Input_Cnt,0) as Odd_input_Cnt, 
case when Valid_Input_Cnt is not null and Correct_Cnt is not null then (Correct_Cnt::float)/(Valid_Input_Cnt::float) 
          when Valid_Input_Cnt is null then -1 
          when Correct_Cnt is null then 0 end as Correct_Rate
from UP as A
left join U1 as B on (A.user_id = B.user_id) 
left join U2 as C on (A.user_id = C.user_id)
left join U3 as D on (A.user_id = D.user_id);
create temporary table U5 as select * ,rank() over (order by correct_rate desc,valid_input_cnt desc,input_cnt ) as Rank, percent_rank() over (order by correct_rate ,valid_input_cnt,input_cnt desc ) as Score from U4 order by Correct_Rate desc ;


-- 取得使用者時間
create temporary table U6 as  select user_id, extract(epoch from max(time)-min(time))/60 as diff_time from govcash_txn group by user_id;

create temporary table U7 as  select A.*,B.diff_time as work_time from U5 as A inner join U6 as B on (A.user_id = B.user_id);

create table UserInfo as select * from U7;	
drop table U1,U2,U3,U4,U6,U7;
-- 計算欄位資料
create temporary table T2 as select page,row,col,sum(Correct_Ind) as Vote_Correct_Cnt, sum(Error_Ind) as Vote_Error_Cnt, count(page)  as Vote_Cnt from T1 group by page,row,col;
create temporary table T3 as select *,(Vote_Correct_Cnt::float)/(Vote_Cnt::float) as Winner_Ratio, 1 as Vote_Type from T2 ;
create temporary table T4 as select A.*,Vote_Type,Winner_Ratio,Vote_Correct_Cnt,Vote_Error_Cnt,Vote_Cnt from A6 as A left join T3 as B on (A.page=B.page and A.row = B.row and A.col=B.col);

-- 剩下還沒表决的
create temporary table T5 as select A.* from B3 as A left join A7 as B on (A.page=B.page and A.row = B.row and A.col=B.col) where B.page is null;
create temporary table T6 as select A.*,B.Correct_Rate from T5 as A left join U5 as B on (A.user_id = B.user_id and B.score>0) where A.ans <> 'FFFFFFFFFFFFF' ;
create temporary table T7 as select page,row,col,ans, 1-exp(sum(log(1-Correct_Rate))) as Guess_Rate,count(*) as Vote_Cnt from T6 group by page,row,col,ans;
--create temporary table T8 as select page,row,col, max( Guess_Rate)  as max_Guess_Rate from T7 group by page,row,col;
--create temporary table T9 as select A.* from T7 as A inner join T8  B on (A.page = B.page and A.col = B.col and A.row=B.row and A.Guess_Rate = B.max_Guess_Rate);
create temporary table T9 as select * from (select *,rank() over (partition by page,row,col order by Guess_rate desc) from T7) as T8 where rank=1;
-- 去除專家決還是無法決定的欄位
create temporary table T99 as select page,row,col from T9 group by page,row,col having count(*)=1;	
create temporary table T10 as select A.* from T9 as A inner join T99 as B on (A.page=B.page and A.row=B.row and A.col=B.col);


-- 結合表格
create temporary table T11 as select page,row,col,count(*) from B3 where ans = 'FFFFFFFFFFFFF' group by page,row,col;
create temporary table T12 as select page,row,col,count(*) from B3 group by page,row,col;
create temporary table  T13 as select 
A.* ,
coalesce(B.ans,C.ans) as ans,
case when B.ans is not null then 1 when C.ans is not null then 2 else 0 end as Vote_Type,
coalesce(B.Winner_Ratio,C.guess_rate) as Winner_Ratio,
coalesce(B.Vote_Cnt,C.Vote_Cnt,E.count) as Vote_Cnt,
coalesce(B.Vote_Correct_Cnt,0) as Vote_Correct_Cnt ,
coalesce(D.count,0) as Vote_Error_Cnt 

from T as A
left join T4 as B on (A.page=B.page and A.row=B.row and A.col=B.col)
left join T10 as C on (A.page=C.page and A.row=C.row and A.col=C.col)
left join T11 as D on (A.page=D.page and A.row=D.row and A.col=D.col)
left join T12 as E on (A.page=E.page and A.row=E.row and A.col=E.col)
;

create table TableColumn as select * from t13;	

-- 輸出

--\copy (select page,row,col from TableColumn where vote_type=0) To '/home/aha/Project/GovCash/data/rerun.csv' With CSV HEADER
--\copy (select * from TableColumn) To '/home/aha/Project/GovCash/data/table_column.csv' With CSV HEADER
--\copy (select * from Votes) To '/home/aha/Project/GovCash/data/vote.csv' With CSV HEADER
--\copy (select * from UserInfo) To '/home/aha/Project/GovCash/data/user_info.csv' With CSV HEADER
