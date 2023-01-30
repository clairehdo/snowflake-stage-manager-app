# This is a Streamlit app that uses Snowpark to connect to Snowflake
# and allows you to explore files in internal stages, as well as create
# and upload files to stages.

# DISCLAIMER
# Ag-Grid [https://pypi.org/project/streamlit-aggrid/] requires 

#!/usr/bin/env python
import streamlit as st
import json
import pandas as pd
from snowflake.snowpark.session import Session
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode

# Set page configuration
st.set_page_config(
    page_title="Snowflake Internal Stage Manager", 
    page_icon="📤", 
    layout="centered")

# FUNCTION: Create Snowflake session
def create_session(connection_parameters):
    if "app_session" not in st.session_state:
        session = Session.builder.configs(connection_parameters).create()
        st.session_state['app_session'] = session

        snowflake_env = session.sql('select current_account(), current_user(), current_role(), current_version(), current_warehouse()').collect()
        st.session_state['app_session_info'] = {
            "account" : connection_parameters['account'],
            "user" : snowflake_env[0][1],
            "role" : snowflake_env[0][2],
            "version" : snowflake_env[0][3],
            "warehouse" : snowflake_env[0][4],
        }
    else:
        session = st.session_state['app_session']
    return session

with st.sidebar:
    st.subheader('Account Credentials')

    if "app_session" not in st.session_state:
        cred_radio = st.radio("Credential location:", ["Use a prepared JSON file", "Manually type in credentials"])
        connection_parameters = {}

        if cred_radio == "Use a prepared JSON file":
            connection_file = st.text_input('JSON File', placeholder='path/to/your/file.json')
            
            try:
                connection_parameters = json.load(open(connection_file))
            except Exception as e:
                st.error(e)

        else:
            connection_parameters = {
                "account": st.text_input('Account', placeholder='ORGID-ACCOUNTNAME'),
                "user": st.text_input('Username', placeholder='username'),
                "password":  st.text_input('Password', placeholder='password', type='password'),
                "role": st.text_input('Role', placeholder='role'),
                "warehouse": st.text_input('Warehouse', placeholder='warehouse')
            }    

        if st.button('Connect to Snowflake'):
            if connection_parameters:
                # Start Snowflake session
                create_session(connection_parameters)
            else:
                st.error('Connection parameters are invalid.')

    if "app_session" in st.session_state:
        st.success("Successfully connected!", icon="🎉")
        st.write("Connection details:")
        st.session_state['app_session_info']

st.title("❄️ Snowflake Internal Stage Manager")

if "app_session" in st.session_state:

    # Instantiate
    st.session_state.dbList = []
    st.session_state.schemaList = []
    st.session_state.stageList = []
    schemaList = []
    database_name = ""
    is_stage_empty = True

    # FUNCTION: Get DB list
    def list_databases():
        st.session_state.dbList = []
        sql = "select database_name from snowflake.information_schema.databases"
        db_sdf = st.session_state['app_session'].sql(sql).collect()
        db_pdf = pd.DataFrame(db_sdf)
        db_list = db_pdf['DATABASE_NAME'].values.tolist()
        st.session_state.dbList = db_list

    # FUNCTION: Get schemas for selected DB
    def list_schemas():
        st.session_state.schemaList = []
        sql = "select schema_name from " + database_name + ".information_schema.schemata"
        schema_sdf = st.session_state['app_session'].sql(sql).collect()
        schema_pdf = pd.DataFrame(schema_sdf)
        schema_list = schema_pdf['SCHEMA_NAME'].values.tolist()
        st.session_state.schemaList = schema_list

    # FUNCTION: Get internal stages for selected DB.Schema
    def list_stages():
        st.session_state.stageList = []
        sql = "select stage_name, stage_type from " + database_name + ".information_schema.stages where stage_schema='" + schema_name + "'  and stage_type='Internal Named'" 
        stage_sdf = st.session_state['app_session'].sql(sql).collect()
        stage_pdf = pd.DataFrame(stage_sdf)
        
        if not stage_pdf.empty:
            # create list for dropdown selectbox
            stage_list = stage_pdf['STAGE_NAME'].values.tolist()
            st.session_state.stageList = stage_list

    list_databases()
    database_name = st.selectbox(
        'Select a DATABASE',
        st.session_state.dbList
    )

    list_schemas()
    schema_name = st.selectbox(
        'Select a SCHEMA',
        st.session_state.schemaList
    )

    tab1, tab2 = st.tabs(["🔍 Stage Explorer", "📤 File Uploader"])

    with tab1:
        tab1.subheader("Explore and delete files in Internal Stages")

        list_stages()

        if len(st.session_state.stageList) > 0:
            stage_name_x = st.selectbox(
                'Select a STAGE',
                st.session_state.stageList,
                key='tab1_stage_selectbox'
            )

            sql = "list @" + database_name + "." + schema_name + "." + stage_name_x
            schema_sdf = st.session_state['app_session'].sql(sql).collect()
            schema_pdf = pd.DataFrame(schema_sdf)

            # If stage has files, proceed.
            if not schema_pdf.empty:
                gb = GridOptionsBuilder.from_dataframe(schema_pdf)
                gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
                gb.configure_selection(selection_mode="multiple", use_checkbox=True)
                gb.configure_side_bar()
                gb.configure_pagination(enabled=True)
                gridoptions = gb.build()

                response = AgGrid(
                    schema_pdf,
                    height=200,
                    gridOptions=gridoptions,
                    enable_enterprise_modules=True,
                    update_mode=GridUpdateMode.MODEL_CHANGED,
                    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                    fit_columns_on_grid_load=False,
                    header_checkbox_selection_filtered_only=True,
                    use_checkbox=True)

                selected_rows = response['selected_rows']
                if selected_rows:
                    st.error('Are you sure you want to delete the selected files? You cannot undo this!', icon="🚨")

                    with st.expander("See selected file(s):"):
                        selected_rows_df = pd.DataFrame(selected_rows)
                        selected_rows_df['name']

                    if st.button("DELETE SELECTED FILE(S)"):
                        sql_list = []
                        
                        # Can only run a single SQL statement at a time
                        df_dict = selected_rows_df.to_dict('records')

                        for row in df_dict:
                            sql_list.append("remove '@" + database_name + "." + schema_name + "." + row['name'] + "'")
                            
                            for sql in sql_list:
                                st.session_state['app_session'].sql(sql).collect()

                    list_stages()
            else:
                st.info("There are no files in the stage '"+ stage_name_x +"'.", icon="ℹ️")

        else:
            st.warning("There are no accessible stages in " + database_name + "." + schema_name + ".", icon="⚠️")
        
    with tab2:
        tab2.subheader("Upload files to an Internal Stage")
        stage_type = st.radio(
            'Do you want to upload to an existing stage or a new stage?',
            ('Upload to an EXISTING stage', 'Upload to a NEW stage')
        )

        if stage_type == 'Upload to an EXISTING stage':
            list_stages()

            if len(st.session_state.stageList) > 0:
                stage_name = st.selectbox(
                    'Select a STAGE',
                    st.session_state.stageList
                )

                is_stage_empty = False
            else:
                st.warning("There are no usable stages in '" + database_name + "." + schema_name + "'.", icon="⚠️")

        if stage_type == 'Upload to a NEW stage':
            stage_name = st.text_input("Enter the name of your new stage. The stage will be automatically created upon uploading your file.", "YOUR_NEW_STAGE_NAME_HERE")
            
            if len(stage_name) > 0:
                is_stage_empty = False
            else:
                is_stage_empty = True

        # Proceed only if user has provided a stage name
        if is_stage_empty is not True:

            stage_path = "@" + database_name + "." + schema_name + "." + stage_name

            is_compress = st.checkbox("Apply compression (GZIP)", True)
            is_overwrite = st.checkbox("Overwrite", True)

            uploaded_file = st.file_uploader(
                "Choose a file to upload to " + stage_path + "",
                accept_multiple_files=True
            )

            if uploaded_file:
                if st.button("Upload Now"):
                    with st.spinner("Uploading"):
                        try:
                            if stage_type == 'Upload to a NEW stage':
                                sql = "create stage if not exists " + database_name + "." + schema_name + "." + stage_name
                                new_stage_sdf = st.session_state['app_session'].sql(sql).collect()
                                st.info(new_stage_sdf[0][0], icon="ℹ️")

                            for file in uploaded_file:
                                file_path = stage_path + "/" + file.name

                                putresult = st.session_state['app_session'].file.put_stream(
                                    file,
                                    file_path,
                                    auto_compress=is_compress,
                                    overwrite=is_overwrite
                                )

                                st.write("PUT Result: ")
                                putresult

                                # Optionally use below to write out only required details
                                # st.write("Source" + putresult[0])
                                # st.write("Target" + putresult[1])
                                # st.write("Source Size" + putresult[2])
                                # st.write("Target Size" + putresult[3])
                                # st.write("Source Compression" + putresult[4])
                                # st.write("Target Compression" + putresult[5])
                                # st.write("Status: " + putresult[6])
                                # st.write("Message: " + putresult[7])
                        except Exception as e:
                            st.error(e)
else:
    st.info("Please use the left sidebar to log into Snowflake.", icon="ℹ️")