## Instalation
Install miniconda or install python 3.10 intepreter

## Miniconda 
```cmd
conda create --name llm-chatbot python=3.10
conda activate llm-chatbot
```

## Install python packages
```cmd
python -m pip install -r requirements.txt -v
```

## Create embedding database
- Run all the cells from chat_custom.ipynb notebook

## Run Streamlit app
```cmd
streamlit run src/frosty_app.py
```

## Chroma Driver installation in windows GCC
https://stackoverflow.com/questions/73969269/error-could-not-build-wheels-for-hnswlib-which-is-required-to-install-pyprojec