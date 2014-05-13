
drop table UserInfo;
drop table TableColumn;
drop table Votes;
drop table govcash_txn_ext;
drop table govcash_txn2;
-- Basic Information

create temporary table T as select distinct page,row,col from govcash_txn order by page,row,col;
create temporary table UP as select  user_id,count(*) as Input_Cnt from govcash_txn group by user_id;

-- Data Cleaning
create temporary table C1 as select A.*,B.from_txndt,B.to_txndt,B.txn_code as txn_code_c from govcash_txn as A left join page as B on (A.page = B.id);
create temporary table C2 as select from_txndt,page,to_txndt,row,col,user_id,txn_no,time,original_ans,txn_code_c,case when length(txn_code_c)>2 and col=3 and txn_code_c is not null and txn_code_c not in ('收入支出','公共關係費用支出','匿名捐贈') and ans != substr(txn_code_c,1,8)  then 'FFFFFFFFFFFFF' else ans end as ans from C1;
create table govcash_txn_ext as select A.*,B.ans as ans1,from_txndt,to_txndt,txn_code_c from govcash_txn as A left join C2 B on (A.txn_no = B.Txn_no);

--排除極少數的Case

create temporary table MapingTalbe as select ans1,ans,txn_Code_c,count(*) from govcash_txn_ext where col=3  group by ans1,ans,txn_Code_c  having count(*)>=5 order by count(*) desc;
--drop table govcash_txn2;
create table govcash_txn2 as select txn_no,page,row,col,user_id,case when col<>3 then A.ans1 when col=3 and B.ans1 is not null then B.ans1 else 'FFFFFFFFFFFFF' end as ans,original_ans,time from govcash_txn_ext as A left join MapingTalbe as B on (A.col=3 and A.ans=B.ans and A.txn_code_C = B.txn_code_C and A.ans1=B.ans1) order by txn_no,page,row,col,user_id;

--create table govcash_txn2 as select txn_no,page,row,col,user_id,ans1 as ans,original_ans,time from govcash_txn_ext order by txn_no,page,row,col,user_id;
drop table C1,C2;

--找出合理的時間
--時間有問題，不能用
--create temporary table temp1 as select txn_no,ans ,from_txndt, to_txndt,string_to_array(ans,'/') as a from C2 where col=2 and ans != 'FFFFFFFFFFFFF' and from_txndt is not null and to_txndt is not null;
--create temporary table temp2 as select txn_no,from_txndt,to_txndt,(to_char((a[1]::integer+1911),'9999')||'-'||a[2]||'-'||a[3])::timestamp as t from temp1;
--create temporary table temp3 as select txn_no,from_txndt,to_txndt,t,case when t between from_txndt and to_txndt then 1 else 0 end as Valid_ind from temp2;

--create temporary table C3 as select A.from_txndt,A.page,A.to_txndt,A.row,A.col,A.user_id,A.txn_no,A.time,A.original_ans,A.txn_code_c,case when col=2 and B.Valid_Ind=0 then 'FFFFFFFFFFFFF' else ans end as ans from C2 as A left join temp3 B on (A.txn_no = B.Txn_no);
--create table govcash_txn_ext as select A.*,B.ans as ans1,from_txndt,to_txndt,txn_code_c from govcash_txn as A left join C3 B on (A.txn_no = B.Txn_no);
--create table govcash_txn2 as select txn_no,page,row,col,user_id,ans1 as ans,original_ans,time from govcash_txn_ext order by txn_no,page,row,col,user_id;

-- create temporary table C3 as select a.page,a.row,a.col,a.user_id,a.ans as ans1,b.ans,b.original_ans,txn_code_c from C2 as A left join govcash_txn B on (A.txn_no =B.txn_no) where A.col=3 and A.ans <>B.ans;



-- 計算使用者資料使用
create temporary table B as select page,row,col,user_id,max(txn_no) as max_txn from govcash_txn2 where ans != 'FFFFFFFFFFFFF' group by page,row,col,user_id;
create temporary table B2 as select  A.* from govcash_txn2 as A inner join B on (A.page =B.page and A.row = B.row and A.col=B.col and  A.user_id = B.user_id and A.txn_no = B.max_txn);

-- 計算欄位資訊使用
create temporary table BB2 as select  * from (select *,rank() over (partition by page,row,col,user_id order by txn_no desc) from govcash_txn2) as A  where rank=1;
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
create temporary table U6 as  select user_id, extract(epoch from max(time)-min(time))/60 as diff_time from govcash_txn2 group by user_id;

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

\copy (select page,row,col from TableColumn where vote_type=0) To '/home/aha/Project/GovCash/data/rerun.csv' With CSV HEADER
\copy (select * from TableColumn) To '/home/aha/Project/GovCash/data/table_column.csv' With CSV HEADER
\copy (select * from Votes) To '/home/aha/Project/GovCash/data/votes.csv' With CSV HEADER
\copy (select * from UserInfo) To '/home/aha/Project/GovCash/data/user_info.csv' With CSV HEADER
\copy (select vote_type,count(*) from tablecolumn group by vote_type order by vote_type) To '/home/aha/Project/GovCash/data/chekc_vote_type.csv' With CSV HEADER
--\copy (select distinct page from govcash_txn_ext where ans <> ans1 and col=2 order by page) To '/home/aha/Project/GovCash/data/check_page_list.csv' With CSV HEADER

