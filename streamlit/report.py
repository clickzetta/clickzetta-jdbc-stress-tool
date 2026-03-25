import pandas as pd
import numpy as np
import streamlit as st
import altair as alt

import os
RENDER_LIMIT = int(os.environ.get('RENDER_LIMIT', '2000'))

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'P{}'.format(n)
    return percentile_

def render_report(df, duration_col):
    df = df.copy()
    duration = df['client_end_ms'].max() - df['client_start_ms'].min()
    qps = 1000.0 * len(df) / duration
    st.code('current sql count {:,} \t time elapsed {:,} ms \t qps {:.3f}'.format(len(df), duration, qps))
    row_count = len(df)
    step = duration // 300

    # align timestamp to x-axis 0
    df['n_client_start_ms'] = df['client_start_ms'] - df['client_start_ms'].min()
    df['n_client_end_ms'] = df['client_end_ms'] - df['client_start_ms'].min()
    df['n_server_submit_ms'] = df['server_submit_ms'] - df['client_start_ms'].min()
    df['n_server_start_ms'] = df['server_start_ms'] - df['client_start_ms'].min()
    df['n_server_end_ms'] = df['server_end_ms'] - df['client_start_ms'].min()

    df['overhead_ms'] = df['client_duration_ms'] - df['server_duration_ms']
    df['server_queue_ms'] = df['server_start_ms'] - df['server_submit_ms']
    df['server_exec_ms'] = df['server_end_ms'] - df['server_start_ms']
    df['gateway_overhead_ms'] = df['gateway_end_ms'] - df['gateway_start_ms'] - df['server_queue_ms'] - df['server_exec_ms']
    df['sdk_overhead_ms'] = df['client_duration_ms'] - (df['client_response_ms'] - df['client_request_ms'])
    df['network_ms'] = df['overhead_ms'] - df['gateway_overhead_ms'] - df['sdk_overhead_ms']

    if row_count >= RENDER_LIMIT:
        st.warning(f'too many data({row_count} rows), no detailed duration distribution chart')
    else: # detailed charts
        df_table = df[['thread_name', 'sql_id', 'job_id', 'is_success', 'result_size',
                'client_duration_ms', 'server_duration_ms',
                'server_queue_ms', 'server_exec_ms']]
        st.markdown('### Duration table')
        st.dataframe(df_table, height=400, use_container_width=True, hide_index=True)

        hint = ['sql_id', 'job_id', 'n_client_start_ms', 'n_client_end_ms', 'client_duration_ms',
                'n_server_submit_ms', 'n_server_start_ms', 'n_server_end_ms', 'server_duration_ms',
                'server_queue_ms', 'server_exec_ms']

        c_client = alt.Chart().mark_bar().encode(
            x='n_client_start_ms',
            x2='n_client_end_ms',
            y='thread_name',
            detail=hint,
            color='sql_id'
        ).interactive()
        c_text = alt.Chart().mark_text(align='left', baseline='middle', color='white').encode(
            x=alt.X('n_client_start_ms'),
            y=alt.Y('thread_name'),
            text=alt.Text('client_duration_ms'),
            detail=hint,
        ).interactive()

        c = alt.layer(c_client, c_text, data=df)
        st.altair_chart(c, use_container_width=True)

    # duration(latency) chart
    st.markdown(f'#### Duration(Latency) Chart: {duration_col}')

    df_duration = df[['sql_id', 'n_client_end_ms', duration_col]]
    df_duration['time'] = df_duration['n_client_end_ms'] // step * step
    df_duration = df_duration.groupby(['time', 'sql_id']).agg(
        {duration_col: ['mean', 'min', 'max', percentile(90), percentile(95), percentile(99)]})
    df_duration.columns = df_duration.columns.map('.'.join)
    df_duration.rename(columns={
                            f'{duration_col}.P90': 'P90',
                            f'{duration_col}.P95': 'P95',
                            f'{duration_col}.P99': 'P99',
                            f'{duration_col}.mean': 'mean',
                            f'{duration_col}.min': 'min',
                            f'{duration_col}.max': 'max'}, inplace=True)
    df_duration = df_duration.reset_index()
    hint = ['time', 'min', 'mean', 'P90', 'P95', 'P99', 'max']
    c = alt.layer(
        alt.Chart(df_duration).mark_point(filled=False).encode(y=alt.Y('mean'), color='sql_id', detail=hint),
        alt.Chart(df_duration).mark_errorbar().encode(y=alt.Y('min', title='duration(ms)'), y2='max', color='sql_id', detail=hint)
    ).encode(
        x=alt.X('time', title='time(ms)')
    ).interactive()
    st.altair_chart(c, use_container_width=True)

    # qps chart
    st.markdown('#### QPS Chart')
    df_qps = df[['n_client_end_ms', 'client_duration_ms']]
    qps_step = step if step >= 1000 else 1000
    df_qps['time'] = df_qps['n_client_end_ms'] // qps_step * qps_step / 1000
    df_qps = df_qps.groupby('time').agg({'client_duration_ms': ['count']})
    df_qps.columns = df_qps.columns.map('.'.join)
    df_qps.rename(columns={'client_duration_ms.count': 'count'}, inplace=True)
    df_qps['qps'] = df_qps['count'] * 1000 / qps_step
    df_qps = df_qps.reset_index()
    c = alt.layer(
        alt.Chart(df_qps).mark_line(point=True).encode(y=alt.Y('qps'))
    ).encode(
        x=alt.X('time', title='time(s)')
    ).interactive()
    st.altair_chart(c, use_container_width=True)

    # profile dataframe
    st.markdown(f'#### SQL Profile Table: {duration_col}')
    stats = df.groupby('sql_id')[duration_col].agg(['count', 'min', 'max', 'mean', 'median'])
    stats['25%'] = df.groupby('sql_id')[duration_col].quantile(0.25)
    stats['75%'] = df.groupby('sql_id')[duration_col].quantile(0.75)
    stats['90%'] = df.groupby('sql_id')[duration_col].quantile(0.90)
    stats['95%'] = df.groupby('sql_id')[duration_col].quantile(0.95)
    stats['99%'] = df.groupby('sql_id')[duration_col].quantile(0.99)
    success_rate = df.groupby('sql_id')['is_success'].mean().rename('success_rate')
    stats = pd.merge(stats, success_rate, on='sql_id').reset_index()

    overall = df[duration_col].agg(['count', 'min', 'max', 'mean', 'median'])
    overall['25%'] = df[duration_col].quantile(0.25)
    overall['75%'] = df[duration_col].quantile(0.75)
    overall['90%'] = df[duration_col].quantile(0.90)
    overall['95%'] = df[duration_col].quantile(0.95)
    overall['99%'] = df[duration_col].quantile(0.99)
    overall['success_rate'] = df['is_success'].mean()
    overall = pd.DataFrame(overall).T
    overall['sql_id'] = '-- OVERALL --'

    stats = pd.concat([overall, stats], ignore_index=True)
    stats['success_rate'] = stats['success_rate'].apply(lambda x: round(x * 100, 2))

    st.dataframe(stats[['sql_id', 'count', 'success_rate', 'min', '25%', 'median', 'mean', '75%', '90%', '95%', '99%', 'max']],
                use_container_width=True, hide_index=True)
