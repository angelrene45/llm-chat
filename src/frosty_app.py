import re

import streamlit as st
import pandas as pd
from snowflake.snowpark.exceptions import SnowparkSQLException

from chain import load_chain, first_instruction

chain = load_chain()

st.title("☃️ Frosty")

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
        else:
            st.markdown(content)


def append_chat_history(question, answer):
    st.session_state["history"].append((question, answer))


def get_sql(text):
    sql_match = re.search(r"```sql\n(.*)\n```", text, re.DOTALL)
    return sql_match.group(1) if sql_match else None


def append_message(content, role="assistant"):
    message = {"role": role, "content": content}
    st.session_state.messages.append(message)

    if not isinstance(content, pd.DataFrame):
        append_chat_history(st.session_state.messages[-2]["content"], content)
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
        content = st.session_state.messages[-1]["content"]
        response = chain(
            {"question": content, "chat_history": st.session_state["history"]}
        )["answer"]
        # response = "```sql\nSELECT error\n```"
        st.markdown(response)

        # for delta in chain:
        #     print("*"*100)
        #     print(delta)
        #     response += delta[1].get("answer", "")

        # # Parse the response for a SQL query and execute if available
        # sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
        # if sql_match:
        #     sql = sql_match.group(1)
        #     conn = st.experimental_connection("snowpark")
        #     message["results"] = conn.query(sql)
        #     st.dataframe(message["results"])

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