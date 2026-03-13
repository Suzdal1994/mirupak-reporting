"""
Алгоритм анализа продаж для ежемесячной отчётности «Мир Упаковки»
Реализует мостик изменения выручки: Эффект объёма + Эффект микса + Эффект цены + Эффект Новые-утраченные
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')


def load_and_validate(filepath: str) -> pd.DataFrame:
    """Загружает Excel-файл и проверяет обязательные столбцы."""
    df = pd.read_excel(filepath, dtype={'Период': str})
    
    required_cols = ['Период', 'Филиал', 'Клиент', 'Сегмент',
                     'Продажи (руб)', 'Продажи (шт)', 'Валовая прибыль (руб)', 'Себестоимость (руб)']
    
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"В файле отсутствуют обязательные столбцы: {missing}")
    
    # Нормализуем период до формата YYYY-MM
    df['Период'] = df['Период'].str.strip().str[:7]
    
    # Числовые столбцы
    num_cols = ['Продажи (руб)', 'Продажи (шт)', 'Валовая прибыль (руб)', 'Себестоимость (руб)']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # R4: исключаем строки с отрицательными продажами
    df = df[df['Продажи (руб)'] >= 0].copy()
    
    # R6: дедупликация — агрегируем по ключу
    df = df.groupby(['Период', 'Филиал', 'Клиент', 'Сегмент'], as_index=False).sum()
    
    return df


def detect_periods(df: pd.DataFrame) -> Tuple[str, str]:
    """Определяет Период_Факт (максимальный) и Период_LY (год назад)."""
    periods = sorted(df['Период'].unique())
    fact_period = periods[-1]
    
    # Вычисляем LY — ровно 12 месяцев назад
    year, month = int(fact_period[:4]), int(fact_period[5:7])
    ly_year = year - 1
    ly_period = f"{ly_year}-{month:02d}"
    
    return fact_period, ly_period


def format_period_name(period: str) -> str:
    """Преобразует 2026-01 → Январь 2026."""
    months = {
        '01': 'Январь', '02': 'Февраль', '03': 'Март', '04': 'Апрель',
        '05': 'Май', '06': 'Июнь', '07': 'Июль', '08': 'Август',
        '09': 'Сентябрь', '10': 'Октябрь', '11': 'Ноябрь', '12': 'Декабрь'
    }
    year, month = period[:4], period[5:7]
    return f"{months.get(month, month)} {year}"


def compute_branch_data(df: pd.DataFrame, branch: str, fact: str, ly: str) -> dict:
    """Вычисляет все метрики для одного филиала."""
    
    df_b = df[df['Филиал'] == branch].copy()
    df_fact = df_b[df_b['Период'] == fact].copy()
    df_ly = df_b[df_b['Период'] == ly].copy()
    
    # ---------- БАЗОВЫЕ МЕТРИКИ ----------
    rev_fact = df_fact['Продажи (руб)'].sum()
    rev_ly = df_ly['Продажи (руб)'].sum()
    vol_fact = df_fact['Продажи (шт)'].sum()
    vol_ly = df_ly['Продажи (шт)'].sum()
    cogs_fact = df_fact['Себестоимость (руб)'].sum()
    cogs_ly = df_ly['Себестоимость (руб)'].sum()
    gp_fact = df_fact['Валовая прибыль (руб)'].sum()
    gp_ly = df_ly['Валовая прибыль (руб)'].sum()
    
    # Клиентская база (только с продажами > 0)
    clients_fact = df_fact[df_fact['Продажи (руб)'] > 0]['Клиент'].nunique()
    clients_ly = df_ly[df_ly['Продажи (руб)'] > 0]['Клиент'].nunique()
    
    avg_ship_fact = rev_fact / clients_fact if clients_fact > 0 else 0
    avg_ship_ly = rev_ly / clients_ly if clients_ly > 0 else 0
    
    # ---------- НОВЫЕ / УТРАЧЕННЫЕ КЛИЕНТЫ (R7) ----------
    clients_fact_set = set(df_fact[df_fact['Продажи (руб)'] > 0]['Клиент'].unique())
    clients_ly_set = set(df_ly[df_ly['Продажи (руб)'] > 0]['Клиент'].unique())
    
    new_clients = clients_fact_set - clients_ly_set
    lost_clients = clients_ly_set - clients_fact_set
    comparable_clients = clients_fact_set & clients_ly_set
    
    # ---------- МОСТИК (R8) ----------
    # Агрегация по клиенту×сегменту для сопоставимой базы
    agg_fact = df_fact.groupby(['Клиент', 'Сегмент']).agg(
        rev=('Продажи (руб)', 'sum'),
        vol=('Продажи (шт)', 'sum')
    ).reset_index()
    
    agg_ly = df_ly.groupby(['Клиент', 'Сегмент']).agg(
        rev=('Продажи (руб)', 'sum'),
        vol=('Продажи (шт)', 'sum')
    ).reset_index()
    
    merged = agg_fact.merge(agg_ly, on=['Клиент', 'Сегмент'], how='outer',
                             suffixes=('_f', '_ly')).fillna(0)
    
    # R5: исключаем аномалии цены из расчёта ценового эффекта
    def price(r, v): return r / v if v > 0 else None
    
    merged['price_f'] = merged.apply(lambda x: price(x['rev_f'], x['vol_f']), axis=1)
    merged['price_ly'] = merged.apply(lambda x: price(x['rev_ly'], x['vol_ly']), axis=1)
    
    # Аномалии: vol=0 но rev>0, или наоборот
    anomaly_mask = (
        ((merged['vol_f'] == 0) & (merged['rev_f'] > 0)) |
        ((merged['vol_f'] > 0) & (merged['rev_f'] == 0)) |
        ((merged['vol_ly'] == 0) & (merged['rev_ly'] > 0)) |
        ((merged['vol_ly'] > 0) & (merged['rev_ly'] == 0))
    )
    comp_base = merged[~anomaly_mask].copy()
    
    # Средняя цена LY по сегменту (для эффекта объёма и микса)
    seg_agg_ly = comp_base[comp_base['vol_ly'] > 0].groupby('Сегмент').agg(
        seg_rev_ly=('rev_ly', 'sum'),
        seg_vol_ly=('vol_ly', 'sum')
    ).reset_index()
    seg_agg_ly['avg_price_ly'] = seg_agg_ly['seg_rev_ly'] / seg_agg_ly['seg_vol_ly']
    
    seg_agg_f = comp_base[comp_base['vol_f'] > 0].groupby('Сегмент').agg(
        seg_rev_f=('rev_f', 'sum'),
        seg_vol_f=('vol_f', 'sum')
    ).reset_index()
    
    seg_combined = seg_agg_f.merge(seg_agg_ly[['Сегмент', 'seg_rev_ly', 'seg_vol_ly', 'avg_price_ly']],
                                    on='Сегмент', how='outer').fillna(0)
    
    # Общая средняя цена LY
    total_vol_ly_comp = comp_base['vol_ly'].sum()
    total_rev_ly_comp = comp_base['rev_ly'].sum()
    overall_avg_price_ly = total_rev_ly_comp / total_vol_ly_comp if total_vol_ly_comp > 0 else 0
    
    # Эффект объёма = (Объём_Факт - Объём_LY) * Средняя_цена_LY (по всей сопоставимой базе)
    total_vol_f_comp = comp_base['vol_f'].sum()
    effect_volume = (total_vol_f_comp - total_vol_ly_comp) * overall_avg_price_ly
    
    # Эффект микса = Σ по сегментам: (доля_сегмента_факт - доля_сегмента_LY) * Объём_Факт * (цена_сегм_LY - средн_цена_LY)
    total_vol_f_comp_safe = total_vol_f_comp if total_vol_f_comp > 0 else 1
    total_vol_ly_comp_safe = total_vol_ly_comp if total_vol_ly_comp > 0 else 1
    
    effect_mix = 0
    for _, row in seg_combined.iterrows():
        share_f = row['seg_vol_f'] / total_vol_f_comp_safe
        share_ly = row['seg_vol_ly'] / total_vol_ly_comp_safe
        seg_price_ly = row['avg_price_ly'] if row['avg_price_ly'] > 0 else overall_avg_price_ly
        effect_mix += (share_f - share_ly) * total_vol_f_comp * (seg_price_ly - overall_avg_price_ly)
    
    # Эффект цены = Σ(цена_Факт - цена_LY) * Объём_Факт (по сопоставимой базе без аномалий)
    effect_price = 0
    for _, row in comp_base.iterrows():
        if row['price_f'] is not None and row['price_ly'] is not None and row['vol_f'] > 0:
            effect_price += (row['price_f'] - row['price_ly']) * row['vol_f']
    
    # Эффект Новые-утраченные
    new_rev = df_fact[df_fact['Клиент'].isin(new_clients)]['Продажи (руб)'].sum()
    lost_rev = df_ly[df_ly['Клиент'].isin(lost_clients)]['Продажи (руб)'].sum()
    effect_new_lost = new_rev - lost_rev
    
    # Эффект изменения себестоимости
    effect_cogs = -(cogs_fact - cogs_ly)
    
    # Сверка
    delta_rev = rev_fact - rev_ly
    bridge_sum = effect_volume + effect_mix + effect_price + effect_new_lost
    discrepancy = delta_rev - bridge_sum  # остаток из-за аномалий
    
    # ---------- ТОП КЛИЕНТЫ ----------
    client_fact = df_fact.groupby('Клиент').agg(
        rev_f=('Продажи (руб)', 'sum'),
        vol_f=('Продажи (шт)', 'sum'),
        gp_f=('Валовая прибыль (руб)', 'sum'),
        cogs_f=('Себестоимость (руб)', 'sum')
    ).reset_index()
    
    client_ly = df_ly.groupby('Клиент').agg(
        rev_ly=('Продажи (руб)', 'sum'),
        vol_ly=('Продажи (шт)', 'sum'),
        gp_ly=('Валовая прибыль (руб)', 'sum'),
        cogs_ly=('Себестоимость (руб)', 'sum')
    ).reset_index()
    
    clients_merged = client_fact.merge(client_ly, on='Клиент', how='outer').fillna(0)
    clients_merged['delta_rev'] = clients_merged['rev_f'] - clients_merged['rev_ly']
    clients_merged['delta_rev_pct'] = clients_merged.apply(
        lambda x: (x['delta_rev'] / x['rev_ly'] * 100) if x['rev_ly'] > 0 else None, axis=1
    )
    clients_merged['delta_gp'] = clients_merged['gp_f'] - clients_merged['gp_ly']
    clients_merged['delta_gp_pct'] = clients_merged.apply(
        lambda x: (x['delta_gp'] / x['gp_ly'] * 100) if x['gp_ly'] > 0 else None, axis=1
    )
    
    # Причина изменения (R9)
    def determine_reason(row):
        if row['rev_ly'] == 0:
            return 'новый клиент'
        if row['rev_f'] == 0:
            return 'утраченный клиент'
        vol_changed = abs(row['vol_f'] - row['vol_ly']) > row['vol_ly'] * 0.05 if row['vol_ly'] > 0 else False
        price_f = row['rev_f'] / row['vol_f'] if row['vol_f'] > 0 else 0
        price_ly = row['rev_ly'] / row['vol_ly'] if row['vol_ly'] > 0 else 0
        price_changed = abs(price_f - price_ly) > price_ly * 0.05 if price_ly > 0 else False
        if vol_changed and price_changed:
            return 'изменение объёма и цены'
        if vol_changed:
            return 'изменение объёма'
        if price_changed:
            return 'изменение цены'
        return 'ограничение данных'
    
    clients_merged['reason'] = clients_merged.apply(determine_reason, axis=1)
    
    # Добавляем основной сегмент клиента
    client_seg = df_fact.groupby('Клиент')['Сегмент'].agg(
        lambda x: x.value_counts().index[0] if len(x) > 0 else 'Н/Д'
    ).reset_index()
    clients_merged = clients_merged.merge(client_seg, on='Клиент', how='left')
    clients_merged['Сегмент'] = clients_merged['Сегмент'].fillna('Н/Д')
    
    # ТОП-10 по |Δвыручка|
    top10_drivers = clients_merged.nlargest(10, 'delta_rev').copy()
    
    # ТОП-15 по выручке Факт
    top15 = clients_merged.nlargest(15, 'rev_f').copy()
    
    # ---------- НОВЫЕ И УТРАЧЕННЫЕ (детализация) ----------
    new_clients_df = df_fact[df_fact['Клиент'].isin(new_clients)].groupby('Клиент').agg(
        rev_f=('Продажи (руб)', 'sum')
    ).reset_index().assign(rev_ly=0)
    new_clients_df['delta'] = new_clients_df['rev_f']
    new_clients_df = new_clients_df.nlargest(15, 'rev_f')
    
    lost_clients_df = df_ly[df_ly['Клиент'].isin(lost_clients)].groupby('Клиент').agg(
        rev_ly=('Продажи (руб)', 'sum')
    ).reset_index().assign(rev_f=0)
    lost_clients_df['delta'] = -lost_clients_df['rev_ly']
    lost_clients_df = lost_clients_df.nlargest(15, 'rev_ly')
    
    # ---------- СЕГМЕНТЫ ----------
    seg_fact = df_fact.groupby('Сегмент').agg(rev_f=('Продажи (руб)', 'sum')).reset_index()
    seg_ly_agg = df_ly.groupby('Сегмент').agg(rev_ly=('Продажи (руб)', 'sum')).reset_index()
    segments = seg_fact.merge(seg_ly_agg, on='Сегмент', how='outer').fillna(0)
    segments['delta_rev'] = segments['rev_f'] - segments['rev_ly']
    segments['delta_pct'] = segments.apply(
        lambda x: (x['delta_rev'] / x['rev_ly'] * 100) if x['rev_ly'] > 0 else None, axis=1
    )
    segments['share_f'] = segments['rev_f'] / rev_fact * 100 if rev_fact > 0 else 0
    segments = segments.nlargest(10, 'rev_f')
    
    return {
        'branch': branch,
        # Базовые метрики
        'rev_fact': rev_fact, 'rev_ly': rev_ly,
        'gp_fact': gp_fact, 'gp_ly': gp_ly,
        'cogs_fact': cogs_fact, 'cogs_ly': cogs_ly,
        'vol_fact': vol_fact, 'vol_ly': vol_ly,
        'clients_fact': clients_fact, 'clients_ly': clients_ly,
        'avg_ship_fact': avg_ship_fact, 'avg_ship_ly': avg_ship_ly,
        # Мостик
        'effect_volume': effect_volume,
        'effect_mix': effect_mix,
        'effect_price': effect_price,
        'effect_new_lost': effect_new_lost,
        'effect_cogs': effect_cogs,
        'discrepancy': discrepancy,
        # Клиенты
        'new_clients_count': len(new_clients),
        'lost_clients_count': len(lost_clients),
        'new_clients_rev': new_rev,
        'lost_clients_rev': lost_rev,
        'top10_drivers': top10_drivers,
        'top15': top15,
        'new_clients_df': new_clients_df,
        'lost_clients_df': lost_clients_df,
        'segments': segments,
    }


def fmt(val, decimals=1) -> str:
    """Форматирует число в тыс.руб. с разделителями."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 'Н/Д'
    return f"{val/1000:,.{decimals}f}".replace(',', ' ')


def fmt_pct(val, decimals=1) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 'Н/Д'
    sign = '+' if val > 0 else ''
    return f"{sign}{val:.{decimals}f}%"


def fmt_rub(val, decimals=0) -> str:
    """Форматирует в рублях (не тыс.)."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 'Н/Д'
    return f"{val:,.{decimals}f}".replace(',', ' ')


def delta_pct(fact, ly) -> str:
    if ly == 0:
        return 'Н/Д'
    return fmt_pct((fact - ly) / abs(ly) * 100)


def effect_share(effect, delta_rev) -> str:
    if delta_rev == 0:
        return 'Н/Д'
    return fmt_pct(effect / abs(delta_rev) * 100)


def generate_branch_markdown(data: dict, fact_period: str, ly_period: str) -> str:
    """Генерирует Markdown для одного филиала (8 слайдов)."""
    
    b = data['branch']
    period_name = format_period_name(fact_period)
    
    d = data
    dr = d['rev_fact'] - d['rev_ly']
    
    lines = []
    
    # === СЛАЙД 1 ===
    lines.append(f"# Основные финансовые метрики {period_name} — {b}")
    lines.append("")
    lines.append("| Показатель | Факт | LY | Δ | Δ% |")
    lines.append("|---|---|---|---|---|")
    
    def row(name, f, l):
        d_ = f - l
        sign = '+' if d_ > 0 else ''
        return f"| {name} | {fmt(f)} | {fmt(l)} | {sign}{fmt(d_)} | {delta_pct(f, l)} |"
    
    def row_rub(name, f, l):
        d_ = f - l
        sign = '+' if d_ > 0 else ''
        return f"| {name} | {fmt_rub(f)} | {fmt_rub(l)} | {sign}{fmt_rub(d_)} | {delta_pct(f, l)} |"
    
    def row_int(name, f, l):
        d_ = f - l
        sign = '+' if d_ > 0 else ''
        return f"| {name} | {int(f):,} | {int(l):,} | {sign}{int(d_):,} | {delta_pct(f, l)} |"
    
    lines.append(row("Выручка, тыс.руб.", d['rev_fact'], d['rev_ly']))
    lines.append(row("Валовая прибыль, тыс.руб.", d['gp_fact'], d['gp_ly']))
    lines.append(row("Себестоимость, тыс.руб.", d['cogs_fact'], d['cogs_ly']))
    lines.append(row_int("Объём, шт", d['vol_fact'], d['vol_ly']))
    lines.append(row_int("Количество клиентов, шт", d['clients_fact'], d['clients_ly']))
    lines.append(row_rub("Средняя отгрузка на 1 клиента, руб", d['avg_ship_fact'], d['avg_ship_ly']))
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # === СЛАЙД 2 — МОСТИК ===
    lines.append(f"# Мостик изменения выручки: вклад эффектов — {b}")
    lines.append("")
    lines.append("| Показатель | Значение, тыс.руб. | Доля в Δвыручки, % |")
    lines.append("|---|---|---|")
    lines.append(f"| Выручка LY | {fmt(d['rev_ly'])} | — |")
    
    def bridge_row(name, effect, delta):
        sign = '+' if effect >= 0 else ''
        share = effect_share(effect, delta)
        return f"| {name} | {sign}{fmt(effect)} | {share} |"
    
    lines.append(bridge_row("+ Эффект объёма", d['effect_volume'], dr))
    lines.append(bridge_row("+ Эффект микса (сегменты)", d['effect_mix'], dr))
    lines.append(bridge_row("+ Эффект цены", d['effect_price'], dr))
    lines.append(bridge_row("+ Эффект новые-утраченные", d['effect_new_lost'], dr))
    lines.append(f"| **= Выручка Факт** | **{fmt(d['rev_fact'])}** | **100%** |")
    lines.append(bridge_row("+ Эффект изменения себестоимости", d['effect_cogs'], dr))
    lines.append(f"| **= Валовая прибыль Факт** | **{fmt(d['gp_fact'])}** | — |")
    lines.append("")
    
    # Ключевые выводы
    effects = {
        'Эффект объёма': d['effect_volume'],
        'Эффект микса': d['effect_mix'],
        'Эффект цены': d['effect_price'],
        'Эффект новые-утраченные': d['effect_new_lost'],
    }
    sorted_effects = sorted(effects.items(), key=lambda x: abs(x[1]), reverse=True)
    
    lines.append("**Ключевые выводы:**")
    lines.append("")
    for i, (name, val) in enumerate(sorted_effects[:2], 1):
        sign = '+' if val >= 0 else ''
        lines.append(f"- Эффект №{i} ({name}): {sign}{fmt(val)} тыс.руб. ({effect_share(val, dr)} от Δвыручки)")
    lines.append(f"- Эффект новые-утраченные: {fmt(d['effect_new_lost'])} тыс.руб. ({effect_share(d['effect_new_lost'], dr)} от Δвыручки)")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # === СЛАЙД 3 — СРЕДНЯЯ ОТГРУЗКА ===
    lines.append(f"# Средняя отгрузка на 1 клиента — {b}")
    lines.append("")
    lines.append("**Динамика:**")
    lines.append("")
    
    d_ship = d['avg_ship_fact'] - d['avg_ship_ly']
    sign = '+' if d_ship >= 0 else ''
    lines.append(f"- Средняя отгрузка: {fmt_rub(d['avg_ship_ly'])} руб → {fmt_rub(d['avg_ship_fact'])} руб (Δ {sign}{fmt_rub(d_ship)} руб; {delta_pct(d['avg_ship_fact'], d['avg_ship_ly'])})")
    
    d_clients = d['clients_fact'] - d['clients_ly']
    if d_clients > 0 and d_ship > 0:
        interp = "рост выручки и рост клиентской базы"
    elif d_clients < 0 and d_ship > 0:
        interp = "рост выручки при сокращении клиентской базы"
    elif d_clients > 0 and d_ship < 0:
        interp = "рост клиентской базы при падении средней отгрузки"
    else:
        interp = "снижение выручки и клиентской базы"
    
    lines.append(f"- Изменение за счёт: {interp}")
    lines.append(f"- Новые клиенты: {d['new_clients_count']} шт. (+{fmt(d['new_clients_rev'])} тыс.руб.); Утраченные: {d['lost_clients_count']} шт. (-{fmt(d['lost_clients_rev'])} тыс.руб.)")
    lines.append("")
    lines.append("| Показатель | Факт | LY | Δ | Δ% |")
    lines.append("|---|---|---|---|---|")
    lines.append(row_int("Количество клиентов, шт", d['clients_fact'], d['clients_ly']))
    lines.append(row("Выручка, тыс.руб.", d['rev_fact'], d['rev_ly']))
    lines.append(row_rub("Средняя отгрузка на 1 клиента, руб", d['avg_ship_fact'], d['avg_ship_ly']))
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # === СЛАЙД 4 — ТОП-10 ДРАЙВЕРЫ ===
    lines.append(f"# Клиенты — драйверы изменения выручки — {b}")
    lines.append("")
    lines.append("| Клиент | Сегмент | Выручка Факт, тыс.руб. | Выручка LY, тыс.руб. | Δвыручка, тыс.руб. | Δ% | Доля в Δвыручки | Причина | Комментарии |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    
    for _, row_ in data['top10_drivers'].iterrows():
        drev = row_['rev_f'] - row_['rev_ly']
        sign = '+' if drev >= 0 else ''
        share = effect_share(drev, dr)
        dpct = delta_pct(row_['rev_f'], row_['rev_ly'])
        seg = row_.get('Сегмент', 'Н/Д')
        lines.append(f"| {row_['Клиент']} | {seg} | {fmt(row_['rev_f'])} | {fmt(row_['rev_ly'])} | {sign}{fmt(drev)} | {dpct} | {share} | {row_['reason']} | |")
    
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # === СЛАЙД 5 — ТОП-15 ===
    lines.append(f"# ТОП-15 клиентов по выручке Факт — {b}")
    lines.append("")
    lines.append("| Клиент | Продажи, тыс.р | Δ продажи, тыс.р | Δ продажи, % | Валовая прибыль, тыс.р | Δ ВП, тыс.р | Δ ВП, % | Комментарии |")
    lines.append("|---|---|---|---|---|---|---|---|")
    
    for _, row_ in data['top15'].iterrows():
        drev = row_['rev_f'] - row_['rev_ly']
        dgp = row_['gp_f'] - row_['gp_ly']
        sign_r = '+' if drev >= 0 else ''
        sign_g = '+' if dgp >= 0 else ''
        dpct_r = delta_pct(row_['rev_f'], row_['rev_ly'])
        dpct_g = delta_pct(row_['gp_f'], row_['gp_ly'])
        lines.append(f"| {row_['Клиент']} | {fmt(row_['rev_f'])} | {sign_r}{fmt(drev)} | {dpct_r} | {fmt(row_['gp_f'])} | {sign_g}{fmt(dgp)} | {dpct_g} | |")
    
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # === СЛАЙД 6 — НОВЫЕ И УТРАЧЕННЫЕ ===
    lines.append(f"# Новые и утраченные клиенты — {b}")
    lines.append("")
    lines.append("## Новые клиенты")
    lines.append("")
    lines.append("| Клиент | Выручка Факт, тыс.руб. | Выручка LY, тыс.руб. | Δвыручка, тыс.руб. |")
    lines.append("|---|---|---|---|")
    
    for _, row_ in data['new_clients_df'].iterrows():
        lines.append(f"| {row_['Клиент']} | {fmt(row_['rev_f'])} | 0 | +{fmt(row_['rev_f'])} |")
    
    lines.append("")
    lines.append(f"**Итого новых клиентов:** {d['new_clients_count']}, вклад в выручку: +{fmt(d['new_clients_rev'])} тыс.руб.")
    lines.append("")
    lines.append("## Утраченные клиенты")
    lines.append("")
    lines.append("| Клиент | Выручка Факт, тыс.руб. | Выручка LY, тыс.руб. | Δвыручка, тыс.руб. |")
    lines.append("|---|---|---|---|")
    
    for _, row_ in data['lost_clients_df'].iterrows():
        lines.append(f"| {row_['Клиент']} | 0 | {fmt(row_['rev_ly'])} | -{fmt(row_['rev_ly'])} |")
    
    lines.append("")
    lines.append(f"**Итого утраченных клиентов:** {d['lost_clients_count']}, потеря выручки: -{fmt(d['lost_clients_rev'])} тыс.руб.")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # === СЛАЙД 7 — СЕГМЕНТЫ ===
    lines.append(f"# Сегменты: ТОП-10 по выручке Факт — {b}")
    lines.append("")
    lines.append("| Сегмент | Выручка Факт, тыс.руб. | Выручка LY, тыс.руб. | Δвыручка, тыс.руб. | Δ% | Доля в выручке Факт | Комментарии |")
    lines.append("|---|---|---|---|---|---|---|")
    
    for _, row_ in data['segments'].iterrows():
        drev = row_['rev_f'] - row_['rev_ly']
        sign = '+' if drev >= 0 else ''
        dpct = delta_pct(row_['rev_f'], row_['rev_ly'])
        share = f"{row_['share_f']:.1f}%"
        lines.append(f"| {row_['Сегмент']} | {fmt(row_['rev_f'])} | {fmt(row_['rev_ly'])} | {sign}{fmt(drev)} | {dpct} | {share} | |")
    
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # === СЛАЙД 8 — ПЛАНОВЫЕ МЕРОПРИЯТИЯ ===
    lines.append(f"# Плановые мероприятия — {b}")
    lines.append("")
    lines.append("*Заполняется сотрудником вручную*")
    lines.append("")
    lines.append("| Мероприятие | Клиент/Сегмент | Ожидаемый эффект (тыс.руб./шт/%) | Ответственный | Срок |")
    lines.append("|---|---|---|---|---|")
    for _ in range(5):
        lines.append("| | | | | |")
    
    return "\n".join(lines)


def generate_summary_markdown(all_data: list, fact_period: str, ly_period: str) -> str:
    """Генерирует сводный Markdown по всем филиалам."""
    period_name = format_period_name(fact_period)
    
    lines = []
    lines.append(f"# Сводная аналитика продаж {period_name} — Все филиалы")
    lines.append("")
    
    # Агрегированные метрики
    total_rev_f = sum(d['rev_fact'] for d in all_data)
    total_rev_ly = sum(d['rev_ly'] for d in all_data)
    total_gp_f = sum(d['gp_fact'] for d in all_data)
    total_gp_ly = sum(d['gp_ly'] for d in all_data)
    total_cogs_f = sum(d['cogs_fact'] for d in all_data)
    total_cogs_ly = sum(d['cogs_ly'] for d in all_data)
    total_vol_f = sum(d['vol_fact'] for d in all_data)
    total_vol_ly = sum(d['vol_ly'] for d in all_data)
    
    lines.append("## Сводные финансовые метрики")
    lines.append("")
    lines.append("| Показатель | Факт | LY | Δ | Δ% |")
    lines.append("|---|---|---|---|---|")
    
    def row_s(name, f, l):
        d_ = f - l
        sign = '+' if d_ > 0 else ''
        dp = delta_pct(f, l)
        return f"| {name} | {fmt(f)} | {fmt(l)} | {sign}{fmt(d_)} | {dp} |"
    
    lines.append(row_s("Выручка, тыс.руб.", total_rev_f, total_rev_ly))
    lines.append(row_s("Валовая прибыль, тыс.руб.", total_gp_f, total_gp_ly))
    lines.append(row_s("Себестоимость, тыс.руб.", total_cogs_f, total_cogs_ly))
    lines.append(f"| Объём, шт | {int(total_vol_f):,} | {int(total_vol_ly):,} | {'+' if total_vol_f > total_vol_ly else ''}{int(total_vol_f - total_vol_ly):,} | {delta_pct(total_vol_f, total_vol_ly)} |")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Рейтинг филиалов
    lines.append("## Рейтинг филиалов по выручке Факт")
    lines.append("")
    lines.append("| Филиал | Выручка Факт, тыс.руб. | Выручка LY, тыс.руб. | Δвыручка, тыс.руб. | Δ% | Доля в холдинге |")
    lines.append("|---|---|---|---|---|---|")
    
    sorted_data = sorted(all_data, key=lambda x: x['rev_fact'], reverse=True)
    for d in sorted_data:
        drev = d['rev_fact'] - d['rev_ly']
        sign = '+' if drev >= 0 else ''
        share = f"{d['rev_fact'] / total_rev_f * 100:.1f}%" if total_rev_f > 0 else 'Н/Д'
        lines.append(f"| {d['branch']} | {fmt(d['rev_fact'])} | {fmt(d['rev_ly'])} | {sign}{fmt(drev)} | {delta_pct(d['rev_fact'], d['rev_ly'])} | {share} |")
    
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Сводный мостик
    lines.append("## Сводный мостик изменения выручки")
    lines.append("")
    total_dr = total_rev_f - total_rev_ly
    total_ev = sum(d['effect_volume'] for d in all_data)
    total_em = sum(d['effect_mix'] for d in all_data)
    total_ep = sum(d['effect_price'] for d in all_data)
    total_enl = sum(d['effect_new_lost'] for d in all_data)
    total_ec = sum(d['effect_cogs'] for d in all_data)
    
    lines.append("| Показатель | Значение, тыс.руб. | Доля в Δвыручки, % |")
    lines.append("|---|---|---|")
    lines.append(f"| Выручка LY | {fmt(total_rev_ly)} | — |")
    
    def br(name, eff, dr):
        sign = '+' if eff >= 0 else ''
        return f"| {name} | {sign}{fmt(eff)} | {effect_share(eff, dr)} |"
    
    lines.append(br("+ Эффект объёма", total_ev, total_dr))
    lines.append(br("+ Эффект микса (сегменты)", total_em, total_dr))
    lines.append(br("+ Эффект цены", total_ep, total_dr))
    lines.append(br("+ Эффект новые-утраченные", total_enl, total_dr))
    lines.append(f"| **= Выручка Факт** | **{fmt(total_rev_f)}** | **100%** |")
    lines.append(br("+ Эффект изменения себестоимости", total_ec, total_dr))
    lines.append(f"| **= Валовая прибыль Факт** | **{fmt(total_gp_f)}** | — |")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Клиентская база по холдингу
    lines.append("## Клиентская база холдинга")
    lines.append("")
    total_clients_f = sum(d['clients_fact'] for d in all_data)
    total_clients_ly = sum(d['clients_ly'] for d in all_data)
    total_new = sum(d['new_clients_count'] for d in all_data)
    total_lost = sum(d['lost_clients_count'] for d in all_data)
    total_new_rev = sum(d['new_clients_rev'] for d in all_data)
    total_lost_rev = sum(d['lost_clients_rev'] for d in all_data)
    
    lines.append(f"- Всего клиентов (Факт): **{total_clients_f:,}** | LY: {total_clients_ly:,} | Δ: {total_clients_f - total_clients_ly:+,}")
    lines.append(f"- Новые клиенты: **{total_new}** (вклад: +{fmt(total_new_rev)} тыс.руб.)")
    lines.append(f"- Утраченные клиенты: **{total_lost}** (потеря: -{fmt(total_lost_rev)} тыс.руб.)")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Плановые мероприятия
    lines.append("## Плановые мероприятия по холдингу")
    lines.append("")
    lines.append("*Заполняется сотрудником вручную*")
    lines.append("")
    lines.append("| Мероприятие | Филиал | Клиент/Сегмент | Ожидаемый эффект (тыс.руб./шт/%) | Ответственный | Срок |")
    lines.append("|---|---|---|---|---|---|")
    for _ in range(5):
        lines.append("| | | | | | |")
    
    return "\n".join(lines)


def run_analysis(filepath: str) -> dict:
    """
    Основная функция. Принимает путь к Excel-файлу.
    Возвращает словарь {имя_файла: markdown_текст}.
    """
    df = load_and_validate(filepath)
    fact_period, ly_period = detect_periods(df)
    
    branches = sorted(df['Филиал'].unique().tolist())
    
    result = {
        'fact_period': fact_period,
        'ly_period': ly_period,
        'period_name': format_period_name(fact_period),
        'branches': branches,
        'files': {}
    }
    
    all_branch_data = []
    for branch in branches:
        branch_data = compute_branch_data(df, branch, fact_period, ly_period)
        all_branch_data.append(branch_data)
        md = generate_branch_markdown(branch_data, fact_period, ly_period)
        filename = f"Аналитика_продаж_{fact_period}_{branch}.md"
        result['files'][filename] = md
    
    summary_md = generate_summary_markdown(all_branch_data, fact_period, ly_period)
    summary_filename = f"Аналитика_продаж_{fact_period}_СВОДНАЯ.md"
    result['files'][summary_filename] = summary_md
    
    return result
