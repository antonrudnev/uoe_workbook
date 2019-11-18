import pandas as pd
import os


def indexes_by_val(df, val):
    vals = [(df[col][df[col].eq(val)].index[i], df.columns.get_loc(col)) for col in df.columns for i in range(len(df[col][df[col].eq(val)].index))]
    if len(vals) == 1:
        return vals[0]
    else:
        raise Exception(f'Ambiguous or None value "{val}"')


def get_val(df, val_name):
    try:
        hrow, hcolumn = indexes_by_val(df, val_name)
        return df.iloc[hrow, hcolumn + 1]
    except:
        return None


def outline_table(df, header_row):
    df.dropna(axis=0, how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)

    hrow, hcolumn = indexes_by_val(df, header_row)
    hname = df.iloc[hrow, hcolumn]
    hsuffix = 0
    for c in range(hcolumn + 1, df.shape[1]):
        v = df.iloc[hrow, c]
        if pd.notna(v):
            hname = v
            df.iloc[hrow, c] = f'{hname}%%VALUE'
            hsuffix = 0
        else:
            df.iloc[hrow, c] = f'{hname}%%VAL_{hsuffix}'
            hsuffix = + 1

    if pd.isna(df.iloc[hrow, hcolumn - 1]):
        while pd.isna(df.iloc[hrow + 1, hcolumn - 1]):
            df.drop(index=df.index[hrow + 1], inplace=True)

    if pd.isna(df.iloc[hrow + 1, hcolumn]):
        df.iloc[hrow + 1, hcolumn:] = df.iloc[hrow, hcolumn:]
        df.drop(index=df.index[hrow], inplace=True)

    df.drop(df.head(hrow).index, inplace=True)

    df.drop(columns=df.columns[pd.isna(df.iloc[0])], inplace=True)

    df.drop(index=df.index[pd.isna(df.iloc[:, 0])], inplace=True)

    df = df.reset_index(drop=True).T.reset_index(drop=True).T
    return df


def pivot_table(df, header_row):
    hrow, hcolumn = indexes_by_val(df, header_row)
    hname = df.iloc[hrow, hcolumn]
    id_vars = df.loc[hrow, :hcolumn - 1].tolist()
    value_vars = df.loc[hrow, hcolumn + 1:].tolist()
    df.drop(df.columns[hcolumn], axis=1, inplace=True)
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name=hname, value_name='_value')

    headers = df[hname].str.split('%%', expand=True)
    df[hname] = headers[0]
    df['_header'] = headers[1]

    df = df.pivot_table(index=id_vars + [hname], columns='_header', values='_value',
                        aggfunc=lambda x: ''.join(str(v) for v in x if pd.notna(v)))

    return df.reset_index()


def transform_to_csv(wb):
    for tab_name in wb:
        ws = wb[tab_name]
        table_name = get_val(ws, 'TABLE_IDENTIFIER')
        breakdown_group = get_val(ws, 'BREAKDOWN_GROUP')
        if table_name and breakdown_group and tab_name not in ['Parameters']:
            time_period = get_val(ws, 'TIME_PERIOD')
            ref_area = get_val(ws, 'REF_AREA')
            ws = outline_table(ws, 'EDUCATION_LEV')
            ws = pivot_table(ws, 'EDUCATION_LEV')
            ws.insert(0,'BREAKDOWN_GROUP', breakdown_group)
            ws.insert(0,'REF_AREA', ref_area)
            ws.insert(0,'TIME_PERIOD', time_period)
            yield (table_name, ws)


if __name__ == '__main__':
    input_dir = 'input'
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for file in [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.lower().endswith('.xlsx')]:
        wb = pd.read_excel(file, sheet_name=None, header=None)
        for table_name, df in transform_to_csv(wb):
            print(file, table_name)
            df.to_csv(os.path.join(output_dir, f'{table_name}.csv'), index=False, header=True)
