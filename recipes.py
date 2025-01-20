import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import pandas as pd
import json
from fpdf import FPDF
from markdown2 import markdown
from bs4 import BeautifulSoup  # To handle HTML parsing

def generate_shopping_list(json_data):
    """
    Generates a shopping list from JSON data, refines it using Snowflake Cortex, 
    and ensures Markdown content is properly formatted for the PDF.
    """
    try:
        # Combine ingredients from all recipes into a single text block
        combined_ingredients = []
        for item in json_data.get('results', []):
            ingredients = item.get('TRANSLATEDINGREDIENTS', "").split(", ")
            combined_ingredients.extend(ingredients)

        # Create a single text input for the Cortex model
        ingredients_text = "\n".join(combined_ingredients)
        prompt = f"""
            You are a smart assistant. Create a comprehensive shopping list in Markdown format 
            (use headers, bullet points, and **bold** where appropriate) based on the following ingredients:
            {ingredients_text}
        """

        # Use Snowflake Cortex to complete the prompt
        cmd = """
                SELECT snowflake.cortex.complete(?, ?) AS response
              """
        df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
        shopping_list_markdown = df_response[0].RESPONSE.strip()

        # Convert Markdown to HTML
        html_text = markdown(shopping_list_markdown)

        # Parse HTML into plain text using BeautifulSoup
        soup = BeautifulSoup(html_text, 'html.parser')
        plain_text = soup.get_text()

        # Initialize the PDF object
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)

        # Add the plain text to the PDF
        pdf.multi_cell(0, 10, plain_text)

        # Convert PDF to binary data
        pdf_data = pdf.output(dest="S").encode("latin1")
        st.session_state.shopping_list_pdf_data = pdf_data

        st.success("Shopping list generated successfully!")

    except Exception as e:
        st.error(f"An error occurred while generating the shopping list: {e}")



pd.set_option("max_colwidth", None)

# Default Values
NUM_CHUNKS = 4  # Number of chunks provided as context. Adjust this to optimize accuracy
SLIDE_WINDOW = 7  # Number of past conversations to remember

# Service parameters
CORTEX_SEARCH_DATABASE = "NUTRITION"
CORTEX_SEARCH_SCHEMA = "DATA"
CORTEX_SEARCH_SERVICE = "FOOD_SEARCH"

# Columns to query in the service
COLUMNS = [
    "TRANSLATEDRECIPENAME",
    "PREPTIMEINMINS",
    "COOKTIMEINMINS",
    "TOTALTIMEINMINS",
    "TRANSLATEDINGREDIENTS",
    "CUISINE",
    "DIET",
    "SERVINGS",
    "TRANSLATEDINSTRUCTIONS",
    "URL"
]


def init_session():

    """
    Initializes and returns a Snowflake session using the Snowpark library.
    """
    connection_params = {
        "account": "DTGLTTL-SVB41214",  # Replace with your Snowflake account identifier
        "user": "Harsh28",  # Replace with your Snowflake username
        "password": "Football@28#",  # Replace with a secure method to retrieve the password
        "role": "ACCOUNTADMIN",
        "database": "NUTRITION",
        "schema": "DATA",
        "warehouse": "COMPUTE_WH",
    }
    try:
        # Create a Snowflake session
        session = Session.builder.configs(connection_params).create()
        st.success("Connected to Snowflake!")
        return session
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None
    

session = init_session()
root = Root(session)

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

### Functions

def config_options():
    st.sidebar.selectbox('Select your model:', (
        'mixtral-8x7b',
        'mistral-large',
        'mistral-7b'), key="model_name")

    # cuisines = session.sql("SELECT DISTINCT CUISINE FROM DATA.FOOD_RECIPIES").collect()
    # diets = session.sql("SELECT DISTINCT DIET FROM DATA.FOOD_RECIPIES").collect()

    # cuisine_list = ['ALL'] + [cuisine.CUISINE for cuisine in cuisines]
    # diet_list = ['ALL'] + [diet.DIET for diet in diets]

    # st.sidebar.selectbox('Select cuisine:', cuisine_list, key="cuisine_value")
    # st.sidebar.selectbox('Select diet:', diet_list, key="diet_value")
    st.sidebar.checkbox('Do you want me to remember the chat history?', key="use_chat_history", value=True)
    st.sidebar.checkbox('Debug: Click to see summary of previous conversations', key="debug", value=True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)

def init_messages():
    # Clear chat messages and reset global variables
    if st.session_state.get("clear_conversation", False) or "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.json_data = None 
        st.session_state.pdf_data = None  # Reset PDF data
        st.session_state.csv_data = None  # Reset CSV data

def fetch_and_store_json_data(question):
    """
    Fetches and stores the JSON data globally in session_state for reuse.
    """
    if "json_data" not in st.session_state:
        st.session_state.json_data = {}  # Initialize if not present

    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history:
            question_summary = summarize_question_with_history(chat_history, question)
            prompt_context = get_similar_chunks_search_service(question_summary)
        else:
            prompt_context = get_similar_chunks_search_service(question)
    else:
        prompt_context = get_similar_chunks_search_service(question)

    # Parse and store JSON data in session state
    json_data = json.loads(prompt_context)
    st.session_state.json_data = json_data



def get_similar_chunks_search_service(query):
    filter_conditions = []

    if st.session_state.cuisine_value != "ALL":
        filter_conditions.append({"@eq": {"CUISINE": st.session_state.cuisine_value}})
    if st.session_state.diet_value != "ALL":
        filter_conditions.append({"@eq": {"DIET": st.session_state.diet_value}})

    if filter_conditions:
        filter_obj = {"@and": filter_conditions}
        response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)
    else:
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)

    st.sidebar.json(response.json())
    return response.json()

def get_chat_history():
    chat_history = []
    start_index = max(0, len(st.session_state.messages) - SLIDE_WINDOW)
    for i in range(start_index, len(st.session_state.messages) - 1):
        chat_history.append(st.session_state.messages[i])
    return chat_history

def summarize_question_with_history(chat_history, question):
    prompt = f"""
        Based on the chat history below and the question, generate a query that extends the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.

        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
    """

    cmd = """
            SELECT snowflake.cortex.complete(?, ?) AS response
          """
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    summary = df_response[0].RESPONSE

    if st.session_state.debug:
        st.sidebar.text("Summary used to find similar chunks in the docs:")
        st.sidebar.caption(summary)

    return summary.replace("'", "")

def download_response_as_pdf(response_text):
    """
    Generates and serves the full response as a downloadable PDF file using a simplified approach.
    """
    try:
        # Initialize the PDF object
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)

        # Add the response text to the PDF
        pdf.multi_cell(0, 10, txt=response_text)  # Handles multi-line text

        # Generate the PDF as a byte string
        pdf_data = pdf.output(dest='S').encode('latin1')

        # Save the PDF data to session_state
        st.session_state.pdf_data = pdf_data

    except Exception as e:
        st.error(f"An error occurred while generating the PDF: {e}")


def download_csv(json_data):
    # Create a list of dictionaries for each recipe
    recipe_data = [
        {
            'Recipe Name': item['TRANSLATEDRECIPENAME'],
            'Ingredients':item['TRANSLATEDINGREDIENTS'],
            'Instructions': item['TRANSLATEDINSTRUCTIONS'],
            'URL': item.get('URL', 'N/A'),
            'Prep Time (mins)': item['PREPTIMEINMINS'],
            'Cook Time (mins)': item['COOKTIMEINMINS'],
            'Total Time (mins)': item['TOTALTIMEINMINS'],
            'Cuisine': item['CUISINE'],
            'Servings': item['SERVINGS'],
            'Diet': item['DIET']
        }
        for item in json_data['results']
    ]

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(recipe_data)

    # Generate the CSV output
    csv_output = df.to_csv(index=False)

    # Save the CSV data to session_state
    st.session_state.csv_data = csv_output

def create_prompt(myquestion):
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history:
            question_summary = summarize_question_with_history(chat_history, myquestion)
            prompt_context = get_similar_chunks_search_service(question_summary)
        else:
            prompt_context = get_similar_chunks_search_service(myquestion)
    else:
        prompt_context = get_similar_chunks_search_service(myquestion)
        chat_history = ""

    prompt = f"""
           You are an expert assistant that extracts information from the CONTEXT provided
           between <context> and </context> tags.
           You offer a chat experience considering the information included in the CHAT HISTORY
           provided between <chat_history> and </chat_history> tags.
           When answering the question contained between <question> and </question> tags,
           be concise and do not hallucinate.
           If you don’t have the information, just say so.

           Do not mention the CONTEXT or CHAT HISTORY used in your answer.

           <chat_history>
           {chat_history}
           </chat_history>
           <context>
           {prompt_context}
           </context>
           <question>
           {myquestion}
           </question>
           Answer:
    """
    return prompt

def complete(myquestion):
    prompt = create_prompt(myquestion)
    cmd = """
            SELECT snowflake.cortex.complete(?, ?) AS response
          """
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    return df_response


# def generate_shopping_list(json_data):
#     """
#     Generates a shopping list from the JSON data, refines it using Snowflake Cortex, 
#     and applies Markdown formatting to the PDF.
#     """
#     try:
#         # Combine ingredients from all recipes into a single text block
#         combined_ingredients = []
#         for item in json_data.get('results', []):
#             ingredients = item.get('TRANSLATEDINGREDIENTS', "").split(", ")
#             combined_ingredients.extend(ingredients)

#         # Create a single text input for the Cortex model
#         ingredients_text = "\n".join(combined_ingredients)
#         prompt = f"""
#             You are a smart assistant. Create a comprehensive shopping list in Markdown format 
#             (use headers, bullet points, and **bold** where appropriate) based on the following ingredients:
#             {ingredients_text}
#         """

#         # Use Snowflake Cortex to complete the prompt
#         cmd = """
#                 SELECT snowflake.cortex.complete(?, ?) AS response
#               """
#         df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
#         shopping_list_markdown = df_response[0].RESPONSE.strip()

#         # Convert Markdown to HTML and then add it to the PDF
#         html_text = markdown(shopping_list_markdown)  # Convert Markdown to HTML

#         # Initialize the PDF object
#         pdf = FPDF()
#         pdf.add_page()
#         pdf.set_font("Arial", size=12)

#         # Parse the HTML and add to PDF line by line
#         for line in html_text.splitlines():
#             pdf.multi_cell(0, 10, line)

#         # Convert PDF to binary data
#         pdf_data = pdf.output(dest="S").encode("latin1")
#         st.session_state.shopping_list_pdf_data = pdf_data

#         st.success("Shopping list generated successfully!")

#     except Exception as e:
#         st.error(f"An error occurred while generating the shopping list: {e}")


def main():
    st.title(":speech_balloon: Chat Assistant with Snowflake Cortex")
    st.write("Explore cuisines and meal plans using structured data:")

    config_options()
    init_messages()

    # Render previous chat messages
    for message in st.session_state.get("messages", []):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Input for new questions
    question = st.chat_input("Enter your question about recipes and cuisines")

    if question:
        st.session_state.messages.append({"role": "user", "content": question})

        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()

            question = question.replace("'", "")

            with st.spinner(f"{st.session_state.model_name} thinking..."):
                response = complete(question)
                fetch_and_store_json_data(question)
                res_text = response[0].RESPONSE
                message_placeholder.markdown(res_text)

        st.session_state.messages.append({"role": "assistant", "content": res_text})
        st.session_state.latest_response = res_text

    # Display buttons to generate and automatically download PDF, CSV, and shopping list
    if "json_data" in st.session_state and st.session_state.json_data:
        if st.button("Generate and Download Shopping List as PDF"):
            generate_shopping_list(st.session_state.json_data)
            st.download_button(
                label="Download Shopping List as PDF",
                data=st.session_state.shopping_list_pdf_data,
                file_name="shopping_list.pdf",
                mime="application/pdf"
            )

        if st.button("Generate and Download Meal Plan as CSV"):
            download_csv(st.session_state.json_data)
            st.download_button(
                label="Download Meal Plan as CSV",
                data=st.session_state.csv_data,
                file_name="mealplan.csv",
                mime="text/csv"
            )

    if st.session_state.get("latest_response") is not None:
        if st.button("Generate and Download Full Response as PDF"):
            download_response_as_pdf(st.session_state.latest_response)
            st.download_button(
                label="Download Full Response as PDF",
                data=st.session_state.pdf_data,
                file_name="recipes.pdf",
                mime="application/pdf"
            )






if __name__ == "__main__":
    main()
