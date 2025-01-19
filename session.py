from snowflake.snowpark import Session

# connection_params = {
#     "account": "DTGLTTL-SVB41214",
#     "user": "Harsh28",
#     "password": "Football@28#", 
#     "role": "ACCOUNTADMIN",
#     "database": "CC_QUICKSTART_CORTEX_SEARCH_DOCS",
#     "schema": "DATA",
#     "warehouse": "COMPUTE_WH",
# }

# session = Session.builder.configs(connection_params).create()

# print("Connected to Snowflake!")

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
    
def main():
    init_session()
