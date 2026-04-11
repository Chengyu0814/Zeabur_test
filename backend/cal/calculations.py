"""華航：四個機場（TPE / TSA / KHH / RMQ）的補貨計算邏輯"""

import math

import pandas as pd


# === 機場設定 ===
# TPE 特殊：庫存來源除了 TPEKNCP 還要加上 TPEKSCI
CAL_AIRPORT_CONFIGS = [
    {'prefix': 'tpe', 'knci': 'TPEKNCP', 'extra_stock': 'TPEKSCI'},
    {'prefix': 'tsa', 'knci': 'TSAKNCI'},
    {'prefix': 'khh', 'knci': 'KHHKNCI'},
    {'prefix': 'rmq', 'knci': 'RMQKNCI'},
]


def calc_single_airport(month_dfs, present_months_sorted, prefix, knci, df_proc_airport, extra_stock_knci=None):
    """計算單一機場的補貨報表"""
    sales_col = f'{knci}_銷售'
    stock_col = f'{knci}_庫存'
    extra_stock_col = f'{extra_stock_knci}_庫存' if extra_stock_knci else None

    load_qty_col = f'{prefix}_裝載量'
    load_set_col = f'{prefix}_裝載套數'
    load_req_col = f'{prefix}_裝載需求量'
    monthly_req_col = f'{prefix}_月銷需求量'
    extra_req_col = f'{prefix}_追加需求量'
    move_req_col = f'{prefix}_待移倉量'

    n_months = len(month_dfs)

    # 合併各月銷售量
    df_merged = month_dfs[0][['PART_NO', sales_col]].copy()
    df_merged = df_merged.rename(columns={sales_col: f'{sales_col}_{present_months_sorted[0]}'})

    for i in range(1, n_months):
        is_last = (i == n_months - 1)
        if is_last:
            cols = ['PART_NO', sales_col, stock_col]
            if extra_stock_col and extra_stock_col in month_dfs[i].columns:
                cols.append(extra_stock_col)
        else:
            cols = ['PART_NO', sales_col]
        df_m = month_dfs[i][cols].copy()
        df_m = df_m.rename(columns={sales_col: f'{sales_col}_{present_months_sorted[i]}'})
        df_merged = df_merged.merge(df_m, on='PART_NO', how='outer')

    # 單月：需從該月取庫存
    if n_months == 1:
        cols = ['PART_NO', stock_col]
        if extra_stock_col and extra_stock_col in month_dfs[0].columns:
            cols.append(extra_stock_col)
        df_merged = df_merged.merge(month_dfs[0][cols].copy(), on='PART_NO', how='outer')

    # TPE 特殊：TPEKSCI + TPEKNCP 庫存合併
    if extra_stock_col and extra_stock_col in df_merged.columns:
        df_merged[stock_col] = df_merged[stock_col].fillna(0) + df_merged[extra_stock_col].fillna(0)
        df_merged.drop(columns=[extra_stock_col], inplace=True)

    # 合併機場採購/裝載資料
    df_cal = df_merged.merge(
        df_proc_airport, left_on='PART_NO', right_on='CI CODE', how='left'
    ).drop(columns=['CI CODE'], errors='ignore')
    df_cal = df_cal.fillna(0)

    # 計算
    df_cal[load_req_col] = df_cal[load_qty_col] * df_cal[load_set_col]

    sales_month_cols = [f'{sales_col}_{m}' for m in present_months_sorted]
    df_cal[monthly_req_col] = df_cal[sales_month_cols].mean(axis=1).round(0).astype('Int64')
    df_cal[extra_req_col] = (df_cal[monthly_req_col] * 1.5).astype('Int64')

    def calc_move_req(row):
        delisting = row.get('規劃性下架', 0)
        if pd.notna(delisting) and str(delisting).strip() not in ('', '0', '0.0'):
            return 0
        x = max(row[load_req_col] + row[monthly_req_col] + row[extra_req_col] - row[stock_col], 0)
        pack = row.get('成箱規定', 0)
        if pd.notna(pack) and float(pack) != 0:
            return math.ceil(x / float(pack)) * float(pack)
        return x

    df_cal[move_req_col] = df_cal.apply(calc_move_req, axis=1).astype('Int64')
    return df_cal


def calc_full_airport_report(month_dfs, present_months_sorted, df_loading, df_procurement, loading_sets, df_inv_data=None, df_cost=None):
    """計算華航四倉完整補貨報表"""

    # 篩選採購大表：只保留裝載表有的品項
    df_proc = df_procurement[df_procurement["CI CODE"].isin(df_loading["CI Code"])].copy().reset_index(drop=True)

    # 在途庫存 mapping by TTB CODE
    if df_inv_data is not None:
        inv_map = df_inv_data.set_index('PART_NO')['在途庫存']
        df_proc['在途庫存'] = df_proc['TTB CODE'].map(inv_map).fillna(0).astype(int)

    # 成本 mapping by TTB CODE
    if df_cost is not None:
        cost_map = df_cost.set_index('PART_NO')['TWD成本']
        df_proc['成本(TWD)'] = df_proc['TTB CODE'].map(cost_map)

    # 為每個機場建立裝載資料
    qty_map = df_loading.set_index('CI Code')['裝載數量']
    q2_map = df_loading.set_index('CI Code')['Q2']

    airport_proc = {}
    for config in CAL_AIRPORT_CONFIGS:
        prefix = config['prefix']
        set_map = loading_sets.get(prefix, {})
        df_ap = df_proc[['CI CODE', 'TTB CODE', '規劃性下架', '成箱規定']].copy()
        df_ap[f'{prefix}_裝載量'] = df_ap['CI CODE'].map(qty_map).fillna(0)
        df_ap[f'{prefix}_裝載套數'] = df_ap['CI CODE'].map(q2_map).map(set_map).fillna(0)
        if '在途庫存' in df_proc.columns:
            df_ap[f'{prefix}_在途庫存'] = df_proc['在途庫存'].values
        if '成本(TWD)' in df_proc.columns:
            df_ap['成本(TWD)'] = df_proc['成本(TWD)'].values
        airport_proc[prefix] = df_ap

    # 計算各機場
    airport_results = {}
    for config in CAL_AIRPORT_CONFIGS:
        prefix = config['prefix']
        airport_results[prefix] = calc_single_airport(
            month_dfs, present_months_sorted,
            prefix, config['knci'], airport_proc[prefix],
            extra_stock_knci=config.get('extra_stock')
        )

    # TPE 特殊計算
    df_tpe = airport_results['tpe']
    df_tpe.drop(columns=['tpe_追加需求量', 'tpe_待移倉量'], inplace=True)

    # 移出量 = 其他三倉的待移倉量加總（用 PART_NO merge 確保對齊）
    move_data = airport_results['tsa'][['PART_NO', 'tsa_待移倉量']].merge(
        airport_results['khh'][['PART_NO', 'khh_待移倉量']], on='PART_NO', how='outer'
    ).merge(
        airport_results['rmq'][['PART_NO', 'rmq_待移倉量']], on='PART_NO', how='outer'
    )
    move_data['tpe_移出量'] = (
        move_data['tsa_待移倉量'].fillna(0) + move_data['khh_待移倉量'].fillna(0) + move_data['rmq_待移倉量'].fillna(0)
    ).astype('Int64')
    df_tpe = df_tpe.merge(move_data[['PART_NO', 'tpe_移出量']], on='PART_NO', how='left')
    df_tpe['tpe_移出量'] = df_tpe['tpe_移出量'].fillna(0).astype('Int64')

    stock_col_tpe = 'TPEKNCP_庫存'

    def calc_purchase(row):
        delisting = row.get('規劃性下架', 0)
        if pd.notna(delisting) and str(delisting).strip() not in ('', '0', '0.0'):
            return 0
        transit = row.get('tpe_在途庫存', 0)
        if pd.isna(transit):
            transit = 0
        x = max(row['tpe_裝載需求量'] + row['tpe_月銷需求量'] + row['tpe_移出量'] - float(transit) - row[stock_col_tpe], 0)
        pack = row.get('成箱規定', 0)
        if pd.notna(pack) and float(pack) != 0:
            return math.ceil(x / float(pack)) * float(pack)
        return x

    df_tpe['tpe_請購量'] = df_tpe.apply(calc_purchase, axis=1).astype('Int64')
    df_tpe['tpe_請購金額'] = (df_tpe['tpe_請購量'] * df_tpe.get('成本(TWD)', pd.Series(dtype=float)).fillna(0)).round(0)
    airport_results['tpe'] = df_tpe

    # 整理 TPE 欄位順序
    knci_tpe = 'TPEKNCP'
    sales_cols_tpe = [f'{knci_tpe}_銷售_{m}' for m in present_months_sorted]
    tpe_ordered = (['PART_NO', 'TTB CODE', '規劃性下架', '成箱規定', '成本(TWD)']
                   + sales_cols_tpe
                   + [f'{knci_tpe}_庫存', 'tpe_裝載量', 'tpe_裝載套數', 'tpe_裝載需求量',
                      'tpe_月銷需求量', 'tpe_在途庫存', 'tpe_移出量', 'tpe_請購量', 'tpe_請購金額'])
    tpe_ordered = [c for c in tpe_ordered if c in df_tpe.columns]
    df_tpe_final = df_tpe[tpe_ordered]

    # 其他機場：去掉共用欄位
    dfs_other = []
    for prefix in ['tsa', 'khh', 'rmq']:
        config = next(c for c in CAL_AIRPORT_CONFIGS if c['prefix'] == prefix)
        knci = config['knci']
        df_ap = airport_results[prefix].copy()
        drop_cols = [c for c in ['TTB CODE', '規劃性下架', '成箱規定', '成本(TWD)', f'{prefix}_在途庫存'] if c in df_ap.columns]
        df_ap.drop(columns=drop_cols, inplace=True)
        dfs_other.append(df_ap)

    # 合併所有機場
    df_final = df_tpe_final
    for df_ap in dfs_other:
        df_final = df_final.merge(df_ap, on='PART_NO', how='left')

    # 只保留裝載表有的品項
    df_final = df_final[df_final['PART_NO'].isin(df_loading['CI Code'])].copy()

    # 加入 SKU 商品名稱
    sku_map = df_loading.drop_duplicates(subset='CI Code').set_index('CI Code')['SKU']
    df_final.insert(2, 'SKU', df_final['PART_NO'].map(sku_map))

    df_final = df_final.sort_values('PART_NO').reset_index(drop=True)
    return df_final
