import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import pandas as pd
import json
from fpdf import FPDF
from markdown2 import markdown
from bs4 import BeautifulSoup  # To handle HTML parsing
import base64
st.set_page_config(page_title='NutriMate', layout = 'wide', page_icon = 'static/NutriMate2.png', initial_sidebar_state = 'auto')

pd.set_option("max_colwidth", None)

# Default Values
NUM_CHUNKS = 4  # Number of chunks provided as context. Adjust this to optimize accuracy
SLIDE_WINDOW = 7  # Number of past conversations to remember

# Service parameters
CORTEX_SEARCH_DATABASE =st.secrets["CORTEX_SEARCH_DATABASE"]
CORTEX_SEARCH_SCHEMA = st.secrets["CORTEX_SEARCH_SCHEMA"]
RECIPE_SEARCH_SERVICE = st.secrets["RECIPE_SEARCH_SERVICE"]
INGREDIENT_SEARCH_SERVICE =st.secrets["INGREDIENT_SEARCH_SERVICE"]
INGREDIENT_BY_NAME_SEARCH_SERVICE =st.secrets["INGREDIENT_BY_NAME_SEARCH_SERVICE"]

# Updated Default Values
TABLE2_COLUMNS = [
    "NAME","CALORIES", "TOTAL_FAT", "CHOLESTEROL", "SODIUM",
    "VITAMIN_A", "VITAMIN_B12", "VITAMIN_B6", "VITAMIN_C",
    "VITAMIN_D", "VITAMIN_E", "VITAMIN_K", "CALCIUM",
    "IRON", "POTASSIUM", "PROTEIN", "CARBOHYDRATE", "CATEGORY"
]

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
    #use the details of your own account for these params
    connection_params = {
        "account":st.secrets["account"],  # Replace with your Snowflake account identifier
        "user": st.secrets["user"],  # Replace with your Snowflake username
        "password": st.secrets["password"], 
        "role": st.secrets["role"],
        "database": st.secrets["database"],
        "schema": st.secrets["schema"],
        "warehouse": st.secrets["warehouse"],
    }
    try:
        # Create a Snowflake session
        session = Session.builder.configs(connection_params).create()
        return session
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None

session = init_session()
root = Root(session)

svcR = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[RECIPE_SEARCH_SERVICE]
svcI_N = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[INGREDIENT_BY_NAME_SEARCH_SERVICE]
svcI = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[INGREDIENT_SEARCH_SERVICE]

### Functions
def add_bg_from_local(image_file):
    """
    Adds a background image to the Streamlit app using a local image file.
    """
    with open(image_file, "rb") as img_file:
        encoded_img = base64.b64encode(img_file.read()).decode()
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("data:image/png;base64,{encoded_img}");
            background-position: center;
            background-size: 78%; /* Keeps the original size of the image */
            background-repeat: no-repeat; /* Repeats the image both horizontally and vertically */
            background-attachment: scroll; /* Ensures it scrolls with the content */
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

# Use the function
add_bg_from_local("static/background.png")

def load_css():
    with open("static/styles.css", "r") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


def config_options():

    st.sidebar.selectbox('Select your model:', (
        'mixtral-8x7b',
        'mistral-large',
        'mistral-7b'), key="model_name")
    st.sidebar.checkbox('Do you want me to remember the chat history?', key="use_chat_history", value=True)
    st.sidebar.checkbox('Debug: Click to see summary of previous conversations', key="debug", value=True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=reset_state)
    #st.sidebar.expander("Session State").write(st.session_state)


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

        #st.success("Shopping list generated successfully!")

    except Exception as e:
        st.error(f"An error occurred while generating the shopping list: {e}")

def extract_ingredients(query):
    """Extracts specific ingredients from the user query."""
    ingredient_prompt = f"List the specific ingredients mentioned in the following query: {query}"
    cmd = """
            SELECT snowflake.cortex.complete(?, ?) AS response
          """
    response = session.sql(
        cmd, params=[st.session_state.model_name, ingredient_prompt]
    ).collect()
    
    if response:
        ingredients = [
            ingredient.strip()
            for ingredient in response[0]["RESPONSE"].split(",")
        ]
        return ingredients
    return []

def fetch_ingredient_details(ingredient):
    """Fetches details for a specific ingredient."""
    detail_prompt = f"Can you tell me about {ingredient}?"
    cmd = """
            SELECT snowflake.cortex.complete(?, ?) AS response
          """
    response = session.sql(
        cmd, params=[st.session_state.model_name, detail_prompt]
    ).collect()
    
    if response:
        return response[0]["RESPONSE"]
    return f"No details found for {ingredient}."




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


def fetch_and_store_json_data(question):
    """
    Fetches and stores the JSON data globally in session_state for reuse.
    """
    classification = classify_prompt(question)
    if classification=='recipe':
        if "json_data" not in st.session_state:
            st.session_state.json_data = {}  # Initialize if not present

        if st.session_state.use_chat_history:
            chat_history = get_chat_history()
            if chat_history:
                question_summary = summarize_question_with_history(chat_history, question)
                prompt_context = get_similar_chunks_search_service(question_summary, classification)
            else:
                prompt_context = get_similar_chunks_search_service(question, classification)
        else:
            prompt_context = get_similar_chunks_search_service(question, classification)

        # Parse and store JSON data in session state
        json_data = json.loads(prompt_context)
        st.session_state.json_data = json_data
        # if "json_data" in st.session_state and st.session_state.json_data:
        #     #st.success("Json data fetched and stored")


def init_messages():
    """
    Clears chat messages and resets all relevant state variables, including flags for generated files.
    """
    if st.session_state.get("clear_conversation", False) or "messages" not in st.session_state:
        # Reset messages
        st.session_state.messages = []

        
def reset_state():
    if st.session_state.get("clear_conversation", False) or "messages" not in st.session_state:
        st.session_state.messages = []
    st.session_state.json_data = None 
    st.session_state.pdf_data = None  # Reset PDF data
    st.session_state.csv_data = None  # Reset CSV data
    st.session_state.latest_response = None

def classify_prompt(query):
    cmd = f"""
    SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
      '{query}',
      [
        {{
          'label': 'recipe',
          'description': 'Queries related to cooking or preparing specific dishes or meals',
          'examples': ['How do I bake a chocolate cake?', 'Give me a recipe for lasagna', 'What are the steps to make sushi?']
        }},
        {{
          'label': 'ingredients',
          'description': 'Queries related to categories of food based on their nutritional facts or general properties',
          'examples': ['What are some high-protein foods?', 'Suggest some low-calorie food categories', 'What should diabetics avoid eating?']
        }},
        {{
          'label': 'ingredients_by_name',
          'description': 'Queries specifically mentioning a named ingredient to get its properties or nutritional facts',
          'examples': ['What are the nutritional facts of mangoes?', 'Tell me about oranges', 'Explain the benefits of bananas.']
        }}
      ],
      {{'task_description': 'Classify the query as recipe, ingredients, or ingredients_by_name based on whether the user asks about preparing a dish, general food properties, or specific ingredient details.'}}
    );
    """

    # Execute the SQL command
    try:
        result = session.sql(cmd).collect()
        
        # Ensure the result is not empty
        if result and result[0]:
            # Parse the JSON string from the result
            data = json.loads(result[0][0])  # Assuming the result is in the first row and column
            label = data.get("label")
            # if label=='ingredients' or label=='ingredients_by_name':
            #     st.session_state.json_data = None
            return label
        else:
            return None  # No classification result
    except Exception as e:
        print(f"Error during classification: {e}")
        return None



def get_similar_chunks_search_service(query, classification):
    if classification == "recipe":
        service = svcR
        query_columns = COLUMNS
    elif classification == "ingredients":
        service = svcI
        query_columns = TABLE2_COLUMNS
    elif classification == "ingredients_by_name":
        service = svcI_N
        query_columns = TABLE2_COLUMNS
    else:
        return {}

    response = service.search(query, query_columns, limit=NUM_CHUNKS)

    
    #st.sidebar.json(response.model_dump_json())
    return response.model_dump_json()


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

    # if st.session_state.debug:
    #     st.sidebar.text("Summary used to find similar chunks in the docs:")
    #     st.sidebar.caption(summary)

    return summary.replace("'", "")


def create_prompt(myquestion):
    classification = classify_prompt(myquestion)
    #st.success(f"Prompt classified as:{classification}")
    if not classification:
        return "Unable to classify the query.", {}

    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history:
            question_summary = summarize_question_with_history(chat_history, myquestion)
            prompt_context = get_similar_chunks_search_service(question_summary, classification)
        else:
            prompt_context = get_similar_chunks_search_service(myquestion, classification)
    else:
        prompt_context = get_similar_chunks_search_service(myquestion, classification)
        chat_history = ""

    if not prompt_context:
        return "No relevant context found.", {}

    # Parse context based on classification
    json_data = json.loads(prompt_context)
    if classification == "recipe":
        results = {item["TRANSLATEDRECIPENAME"]: item["TRANSLATEDINSTRUCTIONS"] for item in json_data["results"]}
    elif classification == "ingredients":
            results = {item["NAME"]: {k: item[k] for k in TABLE2_COLUMNS if k in item} for item in json_data["results"]}
    elif classification == "ingredients_by_name":
            results = {item["NAME"]: {k: item[k] for k in TABLE2_COLUMNS if k in item} for item in json_data["results"]}
    else:
        return "Unknown classification.", {}

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

    return prompt, results

def fetch_and_complete(myquestion):
    prompt, results = create_prompt(myquestion)

    if "ingredients_by_name" in myquestion.lower():
        ingredients = results.keys()
        chat_history = []
        for ingredient in ingredients:
            # Fetch details for each ingredient
            ingredient_prompt = f"Can you tell me about {ingredient}?"
            cmd = """
                SELECT snowflake.cortex.complete(?, ?) AS response
            """
            response = session.sql(
                cmd, params=[st.session_state.model_name, ingredient_prompt]
            ).collect()
            ingredient_details = response[0]["RESPONSE"] if response else "No details found."
            chat_history.append({"role": "user", "content": ingredient_prompt})
            chat_history.append({"role": "assistant", "content": ingredient_details})

    # Use the original question with full chat context
    cmd = """
        SELECT snowflake.cortex.complete(?, ?) AS response
    """
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    res_text = df_response[0]["RESPONSE"] if df_response else "No response received."

    return res_text, results

def complete(myquestion):
    if st.session_state.classification == "ingredients_by_name":
        prompt,recipes = fetch_and_complete(myquestion)
    else:
        prompt, recipes = create_prompt(myquestion)
    cmd = """
            SELECT snowflake.cortex.complete(?, ?) AS response
          """
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    
    # Extract the response text
    res_text = df_response[0]['RESPONSE'] if df_response else "No response received."
    return res_text, recipes

def create_prompt(myquestion):
    classification = classify_prompt(myquestion)
    #st.success(f"Prompt classified as:{classification}")
    if not classification:
        return "Unable to classify the query.", {}

    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history:
            question_summary = summarize_question_with_history(chat_history, myquestion)
            prompt_context = get_similar_chunks_search_service(question_summary, classification)
        else:
            prompt_context = get_similar_chunks_search_service(myquestion, classification)
    else:
        prompt_context = get_similar_chunks_search_service(myquestion, classification)
        chat_history = ""

    if not prompt_context:
        return "No relevant context found.", {}

    # Parse context based on classification
    json_data = json.loads(prompt_context)
    if classification == "recipe":
        results = {item["TRANSLATEDRECIPENAME"]: item["TRANSLATEDINSTRUCTIONS"] for item in json_data["results"]}
    elif classification == "ingredients":
            results = {item["NAME"]: {k: item[k] for k in TABLE2_COLUMNS if k in item} for item in json_data["results"]}
    elif classification == "ingredients_by_name":
            results = {item["NAME"]: {k: item[k] for k in TABLE2_COLUMNS if k in item} for item in json_data["results"]}
    else:
        return "Unknown classification.", {}

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

    return prompt, results


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
        st.session_state.classification = classify_prompt(question)
        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()

            question = question.replace("'", "")

            with st.spinner(f"{st.session_state.model_name} thinking..."):
                res_text, recipes = complete(question)
                fetch_and_store_json_data(question)
                message_placeholder.markdown(res_text)

        st.session_state.messages.append({"role": "assistant", "content": res_text})
        st.session_state.latest_response = res_text

        

    # Display buttons to generate files
    if "json_data" in st.session_state and st.session_state.json_data and st.session_state.classification=="recipe":
        # Generate Shopping List
        if st.button(
            label="Generate Shopping List as PDF",
            icon=":material/shopping_bag:"):
            generate_shopping_list(st.session_state.json_data)
            st.session_state.shopping_list_ready = True
            #st.success("Shopping list PDF generated successfully!")

            # Show download button for Shopping List
            if st.session_state.get("shopping_list_ready"):
                st.download_button(
                    icon=":material/download:",
                    label="Download Shopping List as PDF",
                    data=st.session_state.shopping_list_pdf_data,
                    file_name="shopping_list.pdf",
                    mime="application/pdf"
                )

        # Generate Meal Plan CSV
        if st.button(label="Generate Meal Plan as CSV",icon=":material/ramen_dining:"):
            download_csv(st.session_state.json_data)
            st.session_state.meal_plan_ready = True
            #st.success("Meal plan CSV generated successfully!")

            # Show download button for Meal Plan
            if st.session_state.get("meal_plan_ready"):
                st.download_button(
                    icon=":material/download:",
                    label="Download Meal Plan as CSV",
                    data=st.session_state.csv_data,
                    file_name="mealplan.csv",
                    mime="text/csv"
                )

    # Generate Full Response PDF
    if (st.session_state.get("latest_response") is not None):
        if st.button(label="Generate Full Response as PDF",icon=":material/picture_as_pdf:"):
            download_response_as_pdf(st.session_state.latest_response)
            st.session_state.response_pdf_ready = True
            #st.success("Response PDF generated successfully!")

            # Show download button for Full Response
            if st.session_state.get("response_pdf_ready"):
                st.download_button(
                    icon=":material/download:",
                    label="Download Full Response as PDF",
                    data=st.session_state.pdf_data,
                    file_name="recipes.pdf",
                    mime="application/pdf"
                )

if __name__ == "__main__":
    main()

