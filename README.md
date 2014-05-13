GovCash
=========

找出政治獻金有趣的地方

#資料概要

1. 總共有 671186  欄位
2. 共有  21397  User
3. 3215688 筆資料

#初步分析

1. [人事費用支出 500,1000 總和統計] (https://github.com/ntuaha/GovCash/blob/master/analysis/walker.csv)

#資料來源

- [ronnywang](http://ronny.tw/)
- [G0V政治獻金資料庫](http://campaign-finance.g0v.lackneets.tw/)
- [G0V政治獻金數位化](http://campaign-finance.g0v.ctiml.tw/)

#相關來源
[政治獻金API](http://campaign-finance.g0v.ronny.tw/)

#資料表格式
[Google Doc](https://docs.google.com/spreadsheets/d/15TwXSiI1enBaMWv0WeHTZ3FbLPEmKEAWYGNszDaJXhk/edit#gid=0)

#資料清理

1. 包含去除不符合欄位格式的資料以及不符合欄位應有數值輸入
2. 請參考 [src/load_rawdata_1.py](https://github.com/ntuaha/GovCash/blob/master/src/load_rawdata_1.py)
3. 去除日期不符合應有的格式
4. 數字的全半形轉換
5. **col=8** 限制只能**是, 否, 空白**



 


#資料辨識演算法

1. 選取每個IP對於該欄位的最新輸入作為投票
2. 多數決，可決定的欄位設定Vote_Type=1，剩下平手或者無法決定的欄位**擱置**
3. 針對已經決定的欄位給投票者1分
4. 每位投票者可以根據投票情形得到正確率 = 得分/ 總投票數
5. 針對無法決定的欄位進行所有已經擁有投票的正確率計算，算出每個欄位的正確率 1 - \pi_{\forall user} (1-P(每位投票者正確率))，取出最高的答案，該欄位設定Vote_Type=2
6. (續5)如果同分的答案，則將該欄位設為Vote_type = 0  無法決定
7. 最後每個欄位將分為Vote_Type=1 多數決  Vote_Type=2 權威決  Vote_Type=0 無法決定三個類型

- 建議將Vote_Type的欄位再進行一次辨識
- 原始監察院也有提供不合理的欄位，並非網友輸入有誤（例如：公司統編應為８碼，但卻看見原始資料有超過８碼的可能性)
- 可參閱 [run4.sql](https://github.com/ntuaha/GovCash/blob/master/sql/run4.sql)

###有任何問題請直接回報，會加緊除錯提供更完整的資料表