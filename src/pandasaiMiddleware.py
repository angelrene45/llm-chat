from pandasai.middlewares.base import Middleware

class HandleMathPlotChartsMiddleware(Middleware):
    def run(self, code: str) -> str:

        code = code.replace("plt.show()", "")
        code = code.replace("plt.close('all')", "")
        code = "import streamlit as st\n" + code
        # code = code + """plt.savefig('plot.png')\n"""
        # code = code + """st.session_state.messages.append({"role": "assistant", "content": plt})\n"""
        print(f"Inside Middleware: {code}")
        return code