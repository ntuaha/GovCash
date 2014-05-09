<?php

class ToAccount
{
    public function main()
    {
        $curl = curl_init('http://campaign-finance.g0v.ronny.tw/api/gettables');
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        $ret = curl_exec($curl);
        curl_close($curl);

        $tables = json_decode($ret)->data;

        $page_map = array();
        foreach ($tables as $table) {
            $file = $table->file;
            list($name) = explode('-', $file);
            $id = $table->id;
            $page_map[$id] = $name;
        }

        $files = array();
        $fp = fopen('../data/govcash.csv', 'r');
        fgetcsv($fp); // columns

        $columns = array('交易日期', '收支科目', '捐贈者/支出對象', '身份證/統一編', '收入金額', '支出金額', '金錢類', '地址');

        while ($rows = fgetcsv($fp)) {
            if (!$name = $page_map[$rows[1]]) {
                error_log("找不到 page={$rows[1]}");
                continue;
            }

            if (!$files[$name]) {
                $files[$name] = fopen("../accounts/{$name}.csv", "w");
                fputcsv($files[$name], $columns);
            }
            fputcsv($files[$name], array_slice($rows, 3, -2));
        }
    }
}

$ta = new ToAccount;
$ta->main();
