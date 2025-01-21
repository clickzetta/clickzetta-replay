import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(
    page_title="ClickZetta Replay Result Viewer",
    page_icon=":material/play_arrow:",
    layout="wide",
    # initial_sidebar_state="expanded",
    menu_items = {
        'About': 'https://github.com/clickzetta/clickzetta-replay'
    }
)

col0, col1 = st.columns(2)
csv = col0.text_input('csv file:')
sort_by = col1.selectbox('sort by', ['cz', 'original', 'comparison', 'id'], index=2)
if csv:
    df = pd.read_csv(csv)
    df['comparison'] = df['original'] / df['cz'] * 100
    total = len(df)
    df_succeed = df[df['rs_cnt'] != 'FAILED'].sort_values(sort_by).reset_index(drop=True).reset_index()
    succeed = len(df_succeed)
    df_faster = df[df['comparison'] >= 100]
    faster = len(df_faster)

    cz_stat = f'Avg: {df_succeed.cz.mean():.2f}\tP50: {df_succeed.cz.quantile(0.5):.2f}\tP95: {df_succeed.cz.quantile(0.75):.2f}\tP90: {df_succeed.cz.quantile(0.9):.2f}\tP95: {df_succeed.cz.quantile(0.95):.2f}\tP99: {df_succeed.cz.quantile(0.99):.2f}'
    cz_stat = '\t'.join([f'Avg: {df_succeed.cz.mean():.2f}',
                         f'P50: {df_succeed.cz.quantile(0.5):.2f}',
                         f'P75: {df_succeed.cz.quantile(0.75):.2f}',
                         f'P90: {df_succeed.cz.quantile(0.9):.2f}',
                         f'P95: {df_succeed.cz.quantile(0.95):.2f}',
                         f'P99: {df_succeed.cz.quantile(0.99):.2f}'])
    ori_stat = '\t'.join([f'Avg: {df_succeed.original.mean():.2f}',
                          f'P50: {df_succeed.original.quantile(0.5):.2f}',
                          f'P75: {df_succeed.original.quantile(0.75):.2f}',
                          f'P90: {df_succeed.original.quantile(0.9):.2f}',
                          f'P95: {df_succeed.original.quantile(0.95):.2f}',
                          f'P99: {df_succeed.original.quantile(0.99):.2f}'])
    st.code(f'''Total: {total}\tSucceed: {succeed} ({succeed/total*100:.2f}%)\tFaster: {faster} ({faster/total*100:.2f}%)
Stats:
Clickzetta\t{cz_stat}
Original  \t{ori_stat}''')

    if not df_succeed.empty:
        tooltip=[alt.Tooltip('index', title='index'),
                 alt.Tooltip('id', title='id'),
                 alt.Tooltip('job_id', title='cz job id'),
                 alt.Tooltip('cz', title='cz exec time (ms)'),
                 alt.Tooltip('original', title='original exec time (ms)'),
                 alt.Tooltip('comparison:Q', title='comparison(%)', format='.2f'),]
        c = alt.layer(
            alt.Chart(df_succeed).mark_bar(width=1, align='left').encode(
                y=alt.Y('cz:Q', title=f'execution time(ms)'),
                x=alt.X('index:Q', title=None),
                color=alt.value('steelblue'),
                tooltip=tooltip,
            ) +
            alt.Chart(df_succeed).mark_bar(width=1, align='right').encode(
                y=alt.Y('original:Q', title='execution time(ms)'),
                x=alt.X('index:Q'),
                color=alt.value('salmon'),
                tooltip=tooltip,
            ),
            alt.Chart(df_succeed).mark_line().encode(
                y=alt.Y('comparison:Q', title='comparison %').scale(type='log'),
                x=alt.X('index:Q'),
                color=alt.value('seagreen'),
                tooltip=tooltip,
            ) +
            alt.Chart(df_succeed).mark_rule(strokeDash=[2,2]).encode(
                y=alt.datum(100),
            )
        ).resolve_scale(y='independent').interactive(bind_y=False)
        st.altair_chart(c, use_container_width=True)