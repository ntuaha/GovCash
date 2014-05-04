
drop table u5;
drop table t13;

-- Basic Information

create temporary table T as select distinct page,row,col from govcash_txn order by page,row,col;

create temporary table UP as select  user_id,count(*) as Input_Cnt from govcash_txn group by user_id;


create temporary table B as select page,row,col,user_id,max(txn_no) as max_txn from govcash_txn group by page,row,col,user_id;
create temporary table B2 as select  A.* from govcash_txn as A inner join B on (A.page =B.page and A.row = B.row and A.col=B.col and  A.user_id = B.user_id and A.txn_no = B.max_txn);


-- 計算答案
create temporary table A as select page,row,col,ans,count(user_id) as Cnt  from B2 where ans != 'FFFFFFFFFFFFF' group by page,row,col,ans;  

create temporary table A2 as select page,row,col,max(cnt) as max_cnt,sum(cnt) as sum_cnt from A group by page,row,col having sum(cnt)>1;

create temporary table A3 as select A.*  from A as A inner join A2 as B on (A.page=B.page and A.row=B.row and A.col=B.col and A.cnt = B.max_cnt)  order by page,row,col;
create temporary table A4 as select page,row,col,count(*) as cnt from A3 as A group by page,row,col having count(*)=1;
create temporary table A6 as select A.* from A3 as A inner join A4 as B on (A.page=B.page and A.row=B.row and A.col=B.col);


create temporary table A7 as select distinct page,row,col from A6 ;





-- 正確欄位  錯誤欄位

create temporary table T1 as select A.page,A.row,A.col,A.user_id,A.ans,A.original_ans,A.time,case when B.page is null then 0 else 1 end as Correct_Ind,case when A.ans = 'FFFFFFFFFFFFF' then 1 else 0 end as Error_Ind
from B2 as A
left join A6 as B on  (A.page = B.page and A.row = B.row and A.col= B.col and A.ans= B.ans)
inner join A7 as C on (A.page = C.page and A.row = C.row and A.col= C.col);


-- 使用者資訊


create temporary table U1 as select user_id,count(*) as Valid_Input_Cnt from (select page,row,col,user_id from B2 where ans != 'FFFFFFFFFFFFF') as A group by user_id;
create temporary table U2 as select user_id,count(*) as Correct_Cnt from T1  where Correct_Ind=1 group by user_id;
create temporary table U3 as select user_id,count(*) as Odd_Input_Cnt  from (select page,row,col,user_id from B2 where ans = 'FFFFFFFFFFFFF') as A group by user_id;

create temporary table U4 as select 
A.*, 
case when B.Valid_Input_Cnt is null then 0 else Valid_Input_Cnt end as Valid_input_Cnt,
 case when C.Correct_Cnt is null then 0 else Correct_Cnt end as Correct_Cnt,
case when D.Odd_Input_Cnt is null then 0 else Odd_input_Cnt end as Odd_input_Cnt, 
case when Valid_Input_Cnt is not null and Correct_Cnt is not null then (Correct_Cnt*1.0)/(Valid_Input_Cnt*1.0) 
          when Valid_Input_Cnt is null then -1 
          when Correct_Cnt is null then 0 end as Correct_Rate
  from UP as A
 left join U1 as B on (A.user_id = B.user_id) 
left join U2 as C on (A.user_id = C.user_id)
 left join U3 as D on (A.user_id = D.user_id);
create table U5 as select * ,rank() over (order by correct_rate desc,valid_input_cnt desc,input_cnt ) as Rank, percent_rank() over (order by correct_rate ,valid_input_cnt,input_cnt desc ) as Score from U4 order by Correct_Rate desc ;


-- 取得使用者時間
create temporary table U6 as  select user_id, extract(epoch from max(time)-min(time))/60 as diff_time from govcash_txn group by user_id;

create temporary table U7 as  select A.*,B.diff_time as work_time from U5 as A inner join U6 as B on (A.user_id = B.user_id);

-- 計算欄位資料
create temporary table T2 as select page,row,col,sum(Correct_Ind) as Vote_Correct_Cnt, sum(Error_Ind) as Vote_Error_Cnt, count(page)  as Vote_Cnt from T1 group by page,row,col;
create temporary table T3 as select *,(Vote_Correct_Cnt*1.0)/(Vote_Cnt*1.0) as Winner_Ratio, 1 as Vote_Type from T2 ;
create temporary table T4 as select A.*,Vote_Type,Winner_Ratio,Vote_Correct_Cnt,Vote_Error_Cnt,Vote_Cnt from A6 as A left join T3 as B on (A.page=B.page and A.row = B.row and A.col=B.col);

-- 剩下還沒表决的
create temporary table T5 as select A.* from B2 as A left join A7 as B on (A.page=B.page and A.row = B.row and A.col=B.col) where B.page is null;
create temporary table T6 as select A.*,B.Correct_Rate from T5 as A left join U5 as B on (A.user_id = B.user_id and B.score>0) where A.ans <> 'FFFFFFFFFFFFF' ;
create temporary table T7 as select page,row,col,ans, 1-exp(sum(log(1-Correct_Rate))) as Guess_Rate,count(*) as Vote_Cnt from T6 group by page,row,col,ans;
create temporary table T8 as select page,row,col, max( Guess_Rate)  as max_Guess_Rate from T7 group by page,row,col;
create temporary table T9 as select A.* from T7 as A inner join T8  B on (A.page = B.page and A.col = B.col and A.row=B.row and A.Guess_Rate = B.max_Guess_Rate);
create temporary table T10 as select * from T9 where guess_rate>=0.8;


-- 結合表格
create temporary table T11 as select page,row,col,count(*) from B2 where ans = 'FFFFFFFFFFFFF' group by page,row,col;
create temporary table T12 as select page,row,col,count(*) from B2 group by page,row,col;
create table T13 as select 
A.* ,
case when B.ans is not null then B.ans when C.ans is not null then C.ans else null end as ans,
case when B.ans is not null then 1 when C.ans is not null then 2 else 0 end as Vote_Type,
case when B.Winner_Ratio is not null then B.Winner_Ratio when C.guess_rate is not null then guess_rate else null end as Winner_Ratio,
case when B.Vote_Cnt is not null then B.Vote_Cnt when C.Vote_Cnt is not null then C.Vote_Cnt else E.count end as Vote_Cnt,
case when B.Vote_Correct_Cnt is not null then B.Vote_Correct_Cnt else 0 end as Vote_Correct_Cnt ,
case when D.count is null then 0 else D.count end as Vote_Error_Cnt 

from T as A
left join T4 as B on (A.page=B.page and A.row=B.row and A.col=B.col)
left join T10 as C on (A.page=C.page and A.row=C.row and A.col=C.col)
left join T11 as D on (A.page=D.page and A.row=D.row and A.col=D.col)
left join T12 as E on (A.page=E.page and A.row=E.row and A.col=E.col)
;

-- 輸出

\copy (select page,row,col from t12 where vote_type=0) To '/home/aha/Project/GovCash/res/rerun.csv' With CSV HEADER
