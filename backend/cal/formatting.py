"""華航：輸出報表的欄位格式化（將平面欄位轉為兩層 MultiIndex）"""

import pandas as pd


def build_cal_multi_index(columns, present_months_sorted):
    """將平面欄位轉為 MultiIndex (機場, 指標)"""
    COMMON = {'PART_NO', 'TTB CODE', 'SKU', '規劃性下架', '成箱規定', '成本(TWD)'}
    tuples = []
    for col in columns:
        if col in COMMON:
            tuples.append(('', col))
            continue

        if 'TPE' in col or col.startswith('tpe_'):   top = 'TPE'
        elif 'TSA' in col or col.startswith('tsa_'): top = 'TSA'
        elif 'KHH' in col or col.startswith('khh_'): top = 'KHH'
        elif 'RMQ' in col or col.startswith('rmq_'): top = 'RMQ'
        else:
            tuples.append(('', col))
            continue

        matched = False
        for month in present_months_sorted:
            if col.endswith(f'_{month}'):
                tuples.append((top, f'{month}銷售量'))
                matched = True
                break
        if matched:
            continue

        if '庫存' in col and '在途' not in col:    sub = '庫存數'
        elif '裝載需求量' in col: sub = '裝載需求量'
        elif '裝載套數'  in col: sub = '裝載套數'
        elif '裝載量'   in col: sub = '裝載量'
        elif '月銷需求量' in col: sub = '月銷需求量'
        elif '在途庫存'  in col: sub = '在途庫存'
        elif '追加需求量' in col: sub = '追加需求量'
        elif '待移倉量'  in col: sub = '待移倉量'
        elif '請購金額'  in col: sub = '請購金額'
        elif '請購量'   in col: sub = '請購量'
        elif '移出量'   in col: sub = '移出量'
        else:                    sub = col.split('_')[-1]
        tuples.append((top, sub))

    return pd.MultiIndex.from_tuples(tuples)
