import re
import types
import io 

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from snowflake.snowpark.exceptions import SnowparkSQLException
from pandasai.middlewares.streamlit import StreamlitMiddleware
from pandasai.llm.openai import OpenAI
from pandasai import PandasAI
from matplotlib import pyplot as plt

from chain import load_chain, first_instruction
from pandasaiMiddleware import HandleMathPlotChartsMiddleware

llm = OpenAI()
pandas_ai = PandasAI(llm, middlewares=[HandleMathPlotChartsMiddleware()])
chain = load_chain()

st.title("☃️ Snowflake AI")

# Initialize the chat messages history
if "messages" not in st.session_state:
    # system prompt includes table information, rules, and prompts the LLM to produce
    # a welcome message to the user.
    response = chain(
        {"question": first_instruction, "chat_history": []}
    )["answer"]
    st.session_state.messages = [{"role": "assistant", "content": response}]
    # st.session_state.messages = [{"role": "assistant", "content": "Hello"}]

if "history" not in st.session_state:
    st.session_state["history"] = []

if "text" not in st.session_state:
    st.session_state["text"] = ""

if "df" not in st.session_state:
    st.session_state["df"] = None

# Prompt for user input and save (user messages coming here)
if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})

# display the existing chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        # get content
        content = message["content"]
        # check type of content
        if isinstance(content, pd.DataFrame):
            st.dataframe(content)
        elif isinstance(content, io.BytesIO):
            st.image(content)
        else:
            st.markdown(content)


def append_chat_history(question, answer):
    st.session_state["history"].append((question, answer))


def get_sql(text):
    sql_match = re.search(r"```sql\n(.*)\n```", text, re.DOTALL)
    return sql_match.group(1) if sql_match else None


def append_message(content, role="assistant", skip_history=False):
    message = {"role": role, "content": content}
    st.session_state.messages.append(message)

    if not isinstance(content, pd.DataFrame):
        if not skip_history: append_chat_history(st.session_state.messages[-2]["content"], content)
        # pass


def handle_sql_exception(query, conn, e, retries=2):
    msg_warning = "Uh oh, I made an error, let me try to fix it.."
    
    st.markdown(msg_warning)
    append_message(msg_warning)

    error_message = (
        "I have an SQL query that's causing an error. FIX The SQL query by searching the schema definition:  \n```sql\n"
        + query
        + "\n```\n Error message: \n "
        + str(e)
    )
    new_query = chain({"question": error_message, "chat_history": ""})["answer"]
    st.markdown(new_query)
    append_message(new_query)

    if get_sql(new_query) and retries > 0:
        return execute_sql(get_sql(new_query), conn, retries - 1)
    else:
        append_message("I'm sorry, I couldn't fix the error. Please try again.")
        return None
    

def execute_sql(query, conn, retries=2):
    try:
        conn = st.experimental_connection("snowpark")
        df = conn.query(query)
        return df
    except SnowparkSQLException as e:
        return handle_sql_exception(query, conn, e, retries)

# If last message is not from assistant, we need to generate a new response
if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):

        with st.spinner('Thinking...'):
            content = st.session_state.messages[-1]["content"]
            response = chain(
                {"question": content, "chat_history": st.session_state["history"]}
            )["answer"]
            # response = "```sql\nSELECT country_region, SUM(cases) AS total_cases, SUM(deaths) AS total_deaths FROM ecdc_global GROUP BY country_region;\n```"
            st.markdown(response)
            append_message(response)

        # check if reponse containt sql
        if get_sql(response):
            # execute sql query to snowflake
            conn = st.experimental_connection("snowpark")
            df = execute_sql(get_sql(response), conn)
            if df is not None:
                # append message from AI only to UI but not include dataframe in chat history and avoid send dataframes to openAI
                st.dataframe(df)
                append_message(df)
                st.session_state["df"] = df

                # analyze dataframe
                with st.form("my_form"):
                    st.write('Question Data')
                    text = st.text_input(label="Enter your prompt", key="text")
                    submit_form = st.form_submit_button(label="Ask", help="Click to ask!")

# 
if st.session_state["text"]:
    # show question user from dataframe PandasAI
    st.markdown(st.session_state["text"])
    append_message(st.session_state["text"], role="user", skip_history=True)

    # ask pandasai
    with st.spinner('PandasAI is generating an answer, please wait...'):
        # pandas can provide plot, text or dataframe as answer
        answer = pandas_ai.run(st.session_state["df"], st.session_state["text"])
        
        # get current plots from matplotlib
        fig_number = plt.get_fignums()
        if fig_number:
            # plot answer
            # save image in memory
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png')
            buffer.seek(0)
            st.image(buffer)
            append_message(buffer, role="assistant", skip_history=True)
        elif isinstance(answer, pd.DataFrame):
            # dataframe anwser
            st.dataframe(answer)
            append_message(answer, role="assistant", skip_history=True)
        else:
            # text answer
            st.markdown(answer)
            append_message(answer, role="assistant", skip_history=True)
        

    # clear text and df 
    st.session_state["text"] = ""
    st.session_state["df"] = None
