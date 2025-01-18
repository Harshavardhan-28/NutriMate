
# import streamlit as st

# def upload_to_snowflake(session, file, stage_name="DOCS"):
#     """
#     Uploads a file directly to a Snowflake stage.
#     Args:
#         session: Snowflake session object.
#         file: Uploaded file object from Streamlit file uploader.
#         stage_name: Name of the Snowflake stage.
#     Returns:
#         The relative path of the uploaded file in the stage.
#     """
#     try:
#         # File details
#         file_name = file.name

#         # Use PUT command for uploading (file is a BytesIO object)
#         session.file.put_stream(file, f"@{stage_name}/{file_name}")
#         st.success(f"Uploaded {file_name} to Snowflake stage '{stage_name}'")
#         return file_name  # Relative path is the file name since there’s no subdirectory
#     except Exception as e:
#         st.error(f"Error uploading file: {e}")
#         return None


# def process_document(session, file_path, table_name="DOCS_CHUNKS_TABLE"):
#     """
#     Processes the uploaded document, splits it into chunks, and stores the chunks in a Snowflake table.
#     Args:
#         session: Snowflake session object.
#         file_path: Path of the document to process (relative to the stage).
#         table_name: Name of the Snowflake table to insert chunks into.
#     """
#     try:
#         # Build the query to process and insert document chunks
#         process_query = f"""
#         INSERT INTO {table_name} (relative_path, size, file_url, scoped_file_url, chunk)
#         SELECT
#             relative_path,
#             size,
#             file_url,
#             build_scoped_file_url(@DOCS, relative_path) AS scoped_file_url,
#             func.chunk AS chunk
#         FROM
#             DIRECTORY(@DOCS),
#             TABLE(text_chunker(TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DOCS, 
#                 relative_path, {{'mode': 'LAYOUT'}})))) AS func
#         WHERE relative_path = '{file_path}';
#         """
        
#         # Execute the query
#         session.sql(process_query).collect()
#         st.success(f"Document processed and stored in table: {table_name}")
#     except Exception as e:
#         st.error(f"Error processing document: {e}")




# def main():
#     """
#     Streamlit app main function for document upload and processing.
#     """
#     st.title("Document Upload and Processing in Snowflake")

#     # Initialize Snowflake session
#     session = init_session()
#     if session:
#         # File upload widget
#         uploaded_file = st.file_uploader("Upload your document", type=["txt", "csv", "json", "pdf"])

#         if uploaded_file:
#             # Snowflake stage and table details
#             stage_name = "DOCS"
#             table_name = "DOCS_CHUNKS_TABLE"

#             # Upload file to Snowflake stage
#             file_path = upload_to_snowflake(session, uploaded_file, stage_name)

#             if file_path:
#                 # Automatically process document
#                 process_document(session, file_path, table_name)


# if __name__ == "__main__":
#     main()
from snowflake.snowpark import Session
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import pandas as pd
import json

pd.set_option("max_colwidth", None)

# Default Values
NUM_CHUNKS = 3  # Number of chunks provided as context. Adjust this to optimize accuracy
SLIDE_WINDOW = 7  # Number of past conversations to remember

# Service parameters
CORTEX_SEARCH_DATABASE = "NUTRITION"
CORTEX_SEARCH_SCHEMA = "DATA"
RECIPE_SEARCH_SERVICE = "FOOD_SEARCH"
INGREDIENT_SEARCH_SERVICE = "NUTRITION_SEARCH"

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
    "TOTALTIMEINMINS",
    "CUISINE",
    "DIET",
    "TRANSLATEDINSTRUCTIONS"
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

svcR = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[RECIPE_SEARCH_SERVICE]
svcI = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[INGREDIENT_SEARCH_SERVICE]

### Functions

def config_options():
    st.sidebar.selectbox('Select your model:', (
        'mixtral-8x7b',
        'snowflake-arctic',
        'mistral-large',
        'llama3-8b',
        'llama3-70b',
        'reka-flash',
        'mistral-7b',
        'llama2-70b-chat',
        'gemma-7b'), key="model_name")

    cuisines = session.sql("SELECT DISTINCT CUISINE FROM DATA.RECIPES").collect()
    diets = session.sql("SELECT DISTINCT DIET FROM DATA.RECIPES").collect()

    cuisine_list = ['ALL'] + [cuisine.CUISINE for cuisine in cuisines]
    diet_list = ['ALL'] + [diet.DIET for diet in diets]

    st.sidebar.selectbox('Select cuisine:', cuisine_list, key="cuisine_value")
    st.sidebar.selectbox('Select diet:', diet_list, key="diet_value")
    st.sidebar.checkbox('Do you want me to remember the chat history?', key="use_chat_history", value=True)
    st.sidebar.checkbox('Debug: Click to see summary of previous conversations', key="debug", value=True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)

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
          'description': 'Queries related to specific food items, their properties, or usage in cooking',
          'examples': ['What is a high protein source?', 'What are the nutritionaal facts of oranges per 100g?', 'If I am diabetic what foods should I avoid?']
        }}
      ],
      {{'task_description': 'Classify the query as either recipe or ingredients based on the intent of the user'}}
    );
    """

    # Execute the SQL command
    result = session.sql(cmd).collect()
    
    # Ensure the result is not empty
    if result:
        # Parse the JSON string from the result
        data = json.loads(result[0][0])  # Assuming the result is a single-row, single-column response
        label = data.get("label", "Unknown")
        return label
    else:
        return None  # Return null if no result is returned


def get_similar_chunks_search_service(query, classification):
    if classification == "recipe":
        service = svcR
        query_columns = COLUMNS
    elif classification == "ingredients":
        service = svcI
        query_columns = TABLE2_COLUMNS
    else:
        return {}

    filter_conditions = []
    if st.session_state.cuisine_value != "ALL":
        filter_conditions.append({"@eq": {"CUISINE": st.session_state.cuisine_value}})
    if st.session_state.diet_value != "ALL":
        filter_conditions.append({"@eq": {"DIET": st.session_state.diet_value}})

    if filter_conditions:
        filter_obj = {"@and": filter_conditions}
        response = service.search(query, query_columns, filter=filter_obj, limit=NUM_CHUNKS)
    else:
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


def complete(myquestion):
    prompt, recipes = create_prompt(myquestion)
    cmd = """
            SELECT snowflake.cortex.complete(?, ?) AS response
          """
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    return df_response, recipes

def main():
    st.title(":speech_balloon: Chat Assistant with Snowflake Cortex")
    st.write("Explore Indian food recipes and cuisines using structured data:")

    config_options()
    init_messages()
    
    # for message in st.session_state.get("messages", []):
    #     with st.chat_message(message["role"]):
    #         st.markdown(message["content"])

    # question = st.chat_input("Enter your question about recipes and cuisines")

    # if question:
    #     st.session_state.messages.append({"role": "user", "content": question})

    #     with st.chat_message("user"):
    #         st.markdown(question)

    #     with st.chat_message("assistant"):
    #         message_placeholder = st.empty()

    #         question = question.replace("'", "")

    #         with st.spinner(f"Classifying query..."):
    #             classification = classify_prompt(question)

    #         if not classification:
    #             res_text = "I'm not sure how to classify your question. Please rephrase it."
    #             message_placeholder.markdown(res_text)
    #         else:
    #             with st.spinner(f"Using {classification} search service..."):
    #                 response = get_similar_chunks_search_service(question, classification)

    #             res_text = "Here are the results:" if response else "No relevant results found."
    #             message_placeholder.markdown(res_text)

    #     st.session_state.messages.append({"role": "assistant", "content": res_text})


    for message in st.session_state.get("messages", []):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    question = st.chat_input("Enter your question about recipes and cuisines")

    if question:
        st.session_state.messages.append({"role": "user", "content": question})

        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()

            question = question.replace("'", "")

            with st.spinner(f"{st.session_state.model_name} thinking..."):
                response, recipes = complete(question)
                res_text = response[0].RESPONSE
                message_placeholder.markdown(res_text)

                if recipes:
                    with st.sidebar.expander("Related Recipes"):
                        for name, recipe in recipes.items():
                            st.sidebar.markdown(f"### {name}")
                            st.sidebar.markdown(recipe)

        st.session_state.messages.append({"role": "assistant", "content": res_text})

if __name__ == "__main__":
    main()