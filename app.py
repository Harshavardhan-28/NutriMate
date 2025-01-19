import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import pandas as pd
import json
from fpdf import FPDF
from markdown2 import markdown
from bs4 import BeautifulSoup  # To handle HTML parsing

pd.set_option("max_colwidth", None)

# Default Values
NUM_CHUNKS = 3  # Number of chunks provided as context. Adjust this to optimize accuracy
SLIDE_WINDOW = 7  # Number of past conversations to remember

# Service parameters
CORTEX_SEARCH_DATABASE = "NUTRITION"
CORTEX_SEARCH_SCHEMA = "DATA"
RECIPE_SEARCH_SERVICE = "FOOD_SEARCH"
INGREDIENT_SEARCH_SERVICE = "NUTRITION_SEARCH"
INGREDIENT_BY_NAME_SEARCH_SERVICE = "HARSH"

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

def config_options():
    st.sidebar.selectbox('Select your model:', (
        'mixtral-8x7b',
        'mistral-large',
        'mistral-7b'), key="model_name")

    # cuisines = session.sql("SELECT DISTINCT CUISINE FROM DATA.RECIPES").collect()
    # diets = session.sql("SELECT DISTINCT DIET FROM DATA.RECIPES").collect()

    # cuisine_list = ['ALL'] + [cuisine.CUISINE for cuisine in cuisines]
    # diet_list = ['ALL'] + [diet.DIET for diet in diets]

    # st.sidebar.selectbox('Select cuisine:', cuisine_list, key="cuisine_value")
    # st.sidebar.selectbox('Select diet:', diet_list, key="diet_value")
    st.sidebar.checkbox('Do you want me to remember the chat history?', key="use_chat_history", value=True)
    st.sidebar.checkbox('Debug: Click to see summary of previous conversations', key="debug", value=True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)


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


def init_messages():
    if st.session_state.get("clear_conversation", False) or "messages" not in st.session_state:
        st.session_state.messages = []

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


def create_prompt(myquestion):
    classification = classify_prompt(myquestion)
    st.success(f"Prompt classified as:{classification}")
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


# def complete(myquestion):
#     prompt, recipes = create_prompt(myquestion)
#     cmd = """
#             SELECT snowflake.cortex.complete(?, ?) AS response
#           """
#     df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
#     return df_response, recipes
def complete(myquestion):
    prompt, recipes = create_prompt(myquestion)
    cmd = """
            SELECT snowflake.cortex.complete(?, ?) AS response
          """
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    
    # Extract the response text
    res_text = df_response[0]['RESPONSE'] if df_response else "No response received."
    return res_text, recipes


def main():
    st.title(":speech_balloon: Chat Assistant with Snowflake Cortex")
    st.write("Explore Indian food recipes and cuisines using structured data:")

    config_options()
    init_messages()
    
   
    for message in st.session_state.get("messages", []):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    question = st.chat_input("Enter your question about recipes and cuisines")

    if question:
        # Classify the question
        classification = classify_prompt(question)

        # Append user message to session state
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.markdown(question)

        # Prepare assistant response
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            question = question.replace("'", "")  # Clean the input

            if classification in ["ingredients", "ingredients_by_name"]:
                with st.spinner(f"{st.session_state.model_name} thinking..."):
                    res_text, recipes = complete(question)
                    message_placeholder.markdown(res_text)
                    if recipes:
                        with st.sidebar.expander("Related Recipes"):
                            for name, recipe in recipes.items():
                                st.sidebar.markdown(f"### {name}")
                                st.sidebar.markdown(recipe)

            elif classification == "recipe":
                with st.spinner(f"{st.session_state.model_name} thinking..."):
                    res_text, recipes = complete(question)

                    fetch_and_store_json_data(question)

                    message_placeholder.markdown(res_text)
                # Generate and download files if applicable
                if "json_data" in st.session_state and st.session_state.json_data:
                    col1, col2 = st.columns(2)

                    with col1:
                        if st.button("Generate and Download Shopping List as PDF"):
                            generate_shopping_list(st.session_state.json_data)
                            st.download_button(
                                label="Download Shopping List as PDF",
                                data=st.session_state.shopping_list_pdf_data,
                                file_name="shopping_list.pdf",
                                mime="application/pdf"
                            )

                    with col2:
                        if st.button("Generate and Download Meal Plan as CSV"):
                            download_csv(st.session_state.json_data)
                            st.download_button(
                                label="Download Meal Plan as CSV",
                                data=st.session_state.csv_data,
                                file_name="mealplan.csv",
                                mime="text/csv"
                            )

                if st.session_state.get("latest_response"):
                    if st.button("Generate and Download Full Response as PDF"):
                        download_response_as_pdf(st.session_state.latest_response)
                        st.download_button(
                            label="Download Full Response as PDF",
                            data=st.session_state.pdf_data,
                            file_name="recipes.pdf",
                            mime="application/pdf"
                        )

            else:  # Handle unknown classifications
                res_text = "I'm sorry, I couldn't classify your query. Could you clarify if this is about a recipe, ingredients, or a specific food item?"
                message_placeholder.markdown(res_text)

            # Append assistant response to session state
            st.session_state.messages.append({"role": "assistant", "content": res_text})


    

if __name__ == "__main__":
    main()