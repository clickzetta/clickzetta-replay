import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(
    page_title="ClickZetta Replay Result Viewer",
    # page_icon=icon,
    layout="wide",
    # initial_sidebar_state="expanded",
    menu_items = {
        'About': 'https://github.com/clickzetta/clickzetta-replay'
    }
)

csv = st.text_input('csv file:')
if csv:
    df = pd.read_csv(csv)
    df['comparison'] = df['original'] / df['cz'] * 100
    total = len(df)
    df_succeed = df[df['rs_cnt'] != 'FAILED'].sort_values('comparison').reset_index(drop=True).reset_index()
    succeed = len(df_succeed)
    df_faster = df[df['comparison'] >= 100]
    faster = len(df_faster)

    st.code(f'Total: {total}\tSucceed: {succeed} ({succeed/total*100:.2f}%)\tFaster: {faster} ({faster/total*100:.2f}%)')

    if not df_succeed.empty:
        tooltip=[alt.Tooltip('job_id', title='job_id'),
                 alt.Tooltip('cz', title='cz exec time (ms)'),
                 alt.Tooltip('original', title='original exec time (ms)'),
                 alt.Tooltip('comparison:Q', title='comparison(%)', format='.2f'),]
        c = alt.layer(
            alt.Chart(df_succeed).mark_bar(width=1, align='left').encode(
                y=alt.Y('cz:Q', title=f'execution time(ms)'),
                x=alt.X('index:Q'),
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