import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

# Function to read data from SQL Server into a DataFrame
def read_data_from_sql_server(engine, query):
    df = pd.read_sql(query, engine)
    return df

# Function to update data in the SQL Server table
def update_data_to_sql_server(connection, df, table_name, schema):
    df.to_sql(table_name, con=connection, schema=schema, chunksize=5000, index=False, if_exists='append')

# Define a dictionary to map schemas to their respective tables and columns
schema_tables_mapping = {
    'Petchems': {
        'Company_Lookup': ['Unified_Parent', 'Unified_Company_Name_EN']
    }
}

# Define a dictionary to map databases to their corresponding schemas
database_schemas_mapping = {
    'FilesData': ['Petchems'],
    'Staging': ['API']
}

# Set server name
SERVER_NAME = "RIYD-SQLDB1"

# Set up Streamlit app
st.title("Data Editor")

# Input for SQL Server details
db_connection_string = f"mssql+pyodbc://{SERVER_NAME}/master?driver=ODBC Driver 17 for SQL Server&charset=UTF8"
engine = create_engine(db_connection_string)

# Define the list of databases
databases = ["FilesData", "Staging"]

# Select the database from the list
selected_database = st.selectbox("Select Database:", databases)
db_connection_string = f"mssql+pyodbc://{SERVER_NAME}/{selected_database}?driver=ODBC Driver 17 for SQL Server&charset=UTF8"
engine = create_engine(db_connection_string)

# Get the schemas for the selected database
schemas = database_schemas_mapping[selected_database]

# Select the schema from the list
selected_schema = st.selectbox("Select Schema:", schemas)

# Get the list of tables in the selected schema from the mapping
schema_tables = schema_tables_mapping.get(selected_schema, {})
tables = list(schema_tables.keys())
selected_table = st.selectbox("Select Table:", tables)

# Get the list of columns for the selected table
editable_columns = schema_tables.get(selected_table, [])

# Read data from SQL Server
selected_columns = ", ".join([col for col in editable_columns if col is not None])
if selected_columns:
    query = f"SELECT * FROM {selected_schema}.{selected_table} WHERE Record_Status = 'N' AND "
    conditions = []
    for col in editable_columns:
        if col is not None:
            conditions.append(f"{col} IS NOT NULL")
    query += " AND ".join(conditions)
else:
    query = f"SELECT * FROM {selected_schema}.{selected_table} WHERE Record_Status = 'N'"
#query = f"SELECT * FROM {selected_schema}.{selected_table} where Record_Status = 'N'"

df_original = read_data_from_sql_server(engine, query)
df_editable = df_original.copy()

# Drop columns to exclude from display
st.write("Red Columns for Non-Editable Columns:")

# Select a single editable column for filtering
editable_column = st.selectbox("Select Column for Filtering:", editable_columns)

# Display dropdown for filtering based on distinct values of the selected column
distinct_values = df_editable[editable_column].dropna().unique()
selected_value = st.selectbox(f"Select value for filtering {editable_column}:", ['All'] + list(distinct_values))
if selected_value != 'All':
    filtered_df = df_editable[df_editable[editable_column] == selected_value]
else:
    filtered_df = df_editable

st.dataframe(filtered_df.style.apply(lambda row: ['background-color: #F3B4B1' if col not in editable_columns else '' for col in filtered_df.columns], axis=1))

if st.checkbox("Edit Data"):
    # Display the DataFrame with Streamlit's dataframe function
    for i in range(len(filtered_df)):
        row_input_col = st.columns(len(editable_columns))
        values_entered = []
        for j, column in enumerate(editable_columns):
            if not filtered_df[column].isnull().all():
                new_value = row_input_col[j].text_input(f"Row: {i}, Column: {column}", value=filtered_df.iloc[i][column], key=f"{i}_{column}")
                values_entered.append(new_value)
            else:
                values_entered.append(None)
        # Update the DataFrame with non-empty values
        filtered_df.loc[i, editable_columns] = values_entered
    
    if st.button("Submit Changes"):
        # Display the updated DataFrame
        st.write(filtered_df) 

    # Save changes button
    if st.button("Save Changes"):
        connection = engine.connect()
        update_data_to_sql_server(connection, filtered_df, selected_table, selected_schema)
        connection.commit()  # Commit the changes
        connection.close()
        st.success("Changes saved successfully!")

# Close the SQLAlchemy engine
engine.dispose()
