import streamlit as st
from multiapp import MultiApp
from views import home,data

st.set_page_config(page_title="Slack Data Analysis")

app = MultiApp()


st.title("Slack Data Analysis")

st.markdown("""
            # Message Data Analysis
            Slack Data Analysis of Messages
            """)

app.add_app("Home", home.app)
app.add_app("Data", data.app)
#app.add_app("Visualization", visualization.app)

#  main app
app.run()
