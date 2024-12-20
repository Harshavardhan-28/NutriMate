import streamlit as st
from snowflake.snowpark import Session

# Initialize Snowflake session
def init_session():
    connection_params = {
        "account": "*",
        "user": "*",
        "password": "*",  # Replace with a secure method to retrieve the password
        "role": "ACCOUNTADMIN",
        "database": "CC_QUICKSTART_CORTEX_SEARCH_DOCS",
        "schema": "DATA",
        "warehouse": "COMPUTE_WH",
    }
    try:
        session = Session.builder.configs(connection_params).create()
        st.success("Connected to Snowflake!")
        return session
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None

def upload_to_snowflake(session, file, stage_name="DOCS", path="documents/"):
    """
    Uploads a file to a Snowflake stage.
    """
    try:
        # File details
        file_name = file.name

        # Use PUT command for uploading (file itself is a BytesIO object)
        session.file.put_stream(file, f"@{stage_name}/{path}{file_name}")
        st.success(f"Uploaded {file_name} to Snowflake stage '{stage_name}/{path}'")
        return f"{path}{file_name}"
    except Exception as e:
        st.error(f"Error uploading file: {e}")
        return None


def process_document(session, file_path, table_name="DOCS_CHUNKS_TABLE"):
    """
    Processes the uploaded document, splits it into chunks, and stores the chunks in a Snowflake table.
    """
    try:
        # Processing query
        process_query = f"""
        COPY INTO {table_name}
        FROM @DOCS/documents/{file_path}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"')
        ON_ERROR = CONTINUE;
        """
        session.sql(process_query).collect()
        st.success(f"Document processed and stored in table: {table_name}")
    except Exception as e:
        st.error(f"Error processing document: {e}")

def main():
    st.title("Document Upload and Processing in Snowflake")

    session = init_session()
    if session:
        # File upload widget
        uploaded_file = st.file_uploader("Upload your document", type=["txt", "csv", "json", "pdf"])

        if uploaded_file:
            # Snowflake stage and table details
            stage_name = "DOCS"
            table_name = "DOCS_CHUNKS_TABLE"

            # Upload file to Snowflake stage
            file_path = upload_to_snowflake(session, uploaded_file, stage_name)

            if file_path:
                # Automatically process document
                process_document(session, file_path, table_name)

if __name__ == "__main__":
    main()
