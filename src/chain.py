import streamlit as st

from langchain.prompts.prompt import PromptTemplate
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from langchain.chains import ConversationalRetrievalChain, LLMChain
from langchain.chains.question_answering import load_qa_chain
from langchain.chat_models import ChatOpenAI
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.vectorstores import Chroma

template_questions = """Considering the provided chat history and a subsequent question, rewrite the follow-up question to be an independent query. Alternatively, conclude the conversation if it appears to be complete.
Chat History:\"""
{chat_history}
\"""
Follow Up Input: \"""
{question}
\"""
Standalone question:"""


template_qa = """ 
You will be acting as an AI Snowflake SQL Expert. 
Your goal is to give correct, executable sql query to users.
When providing responses, strive to exhibit friendliness and adopt a conversational tone, similar to how a friend or tutor would communicate.
When asked about your capabilities, provide a general overview of your ability to assist with data analysis tasks using Snowflake SQL, instead of performing specific SQL queries. 
Based on the question provided, if it pertains to data analysis or SQL tasks, generate SQL code that is compatible with the Snowflake environment with limit 10. Additionally, offer a brief explanation about how you arrived at the SQL code. If the required column isn't explicitly stated in the context, suggest an alternative using available columns, but do not assume the existence of any columns that are not mentioned. Also, do not modify the database in any way (no insert, update, or delete operations). You are only allowed to query the database. Refrain from using the information schema.
If the question or context does not clearly involve SQL or data analysis tasks, respond appropriately without generating SQL queries. 
When the user expresses gratitude or says "Thanks", interpret it as a signal to conclude the conversation. Respond with an appropriate closing statement without generating further SQL queries.
If you don't know the answer, simply state, "I'm sorry, I don't know the answer to your question."
Don't forget to use "ilike %keyword%" for fuzzy match queries (especially for variable_name column)
Write your response in markdown format.

Question: ```{question}```
{context}

Answer:
"""

first_instruction = """Now to get started, please briefly introduce yourself, describe the database at a high level. Then provide 3 example questions using bullet points. this reponse without query. Write your response in markdown format."""

condense_question_prompt = PromptTemplate.from_template(template_questions)
prompt_qa = PromptTemplate(template=template_qa, input_variables=["question", "context"])

def get_chain(vectorstore):
    """
    Get a chain for chatting with a vector database.
    """
    q_llm = ChatOpenAI(
        temperature=0.1,
        openai_api_key=st.secrets["OPENAI_API_KEY"],
        model_name="gpt-3.5-turbo-16k",
        max_tokens=500,
    )

    streaming_llm = ChatOpenAI(
        model_name="gpt-3.5-turbo",
        temperature=0.5,
        openai_api_key=st.secrets["OPENAI_API_KEY"],
        max_tokens=500,
        streaming=True,
    )

    question_generator = LLMChain(llm=q_llm, prompt=condense_question_prompt)

    doc_chain = load_qa_chain(llm=streaming_llm, chain_type="stuff", prompt=prompt_qa)
    conv_chain = ConversationalRetrievalChain(
        retriever=vectorstore.as_retriever(),
        combine_docs_chain=doc_chain,
        question_generator=question_generator
    )

    return conv_chain


def load_chain():
    """
    Load the chain from the local file system

    Returns:
        chain (Chain): The chain object

    """
    embeddings = OpenAIEmbeddings(
        openai_api_key=st.secrets["OPENAI_API_KEY"], model="text-embedding-ada-002"
    )
    vector_store = Chroma(
        persist_directory="chroma_db", 
        embedding_function=embeddings
    )
    return get_chain(vector_store)