import streamlit as st
import mysql.connector
import pandas as pd
import secrets
import string
import numpy as np

connection = mysql.connector.connect(
    host=st.secrets["DATABASE_HOST"],
    port=st.secrets["DATABASE_PORT"],
    user=st.secrets["DATABASE_USER"],
    password=st.secrets["DATABASE_PASSWORD"],
    database=st.secrets["DATABASE_NAME"]
)

def get_df_anmeldung(pconnection: mysql.connector.MySQLConnection):
    query_anmeldung = "SELECT * FROM `Anmeldung_test` WHERE `deleted` = 0 AND `confirmed` = 1"
    df_anmeldung = pd.read_sql(query_anmeldung, pconnection)
    return df_anmeldung

def get_df_kgtn(pconnection: mysql.connector.MySQLConnection):
    query_kg_teilnehmer = "SELECT * FROM `KGTN`"
    df_kgtn = pd.read_sql(query_kg_teilnehmer, pconnection)
    return df_kgtn

def get_df_kleingruppen(pconnection: mysql.connector.MySQLConnection):
    query_kg_teilnehmer = "SELECT * FROM `Kleingruppen`"
    df_kgtn = pd.read_sql(query_kg_teilnehmer, pconnection)
    return df_kgtn

def get_random_password():
    random_password = ''.join(secrets.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for i in range(15))
    random_password = "-".join(map("".join, zip(*[iter(random_password)]*5)))
    return random_password

def get_new_username(pconnection):
    kleingruppen = get_df_kleingruppen(pconnection)
    updated_username = f"Kleingruppe{len(kleingruppen)+1:03}"
    return updated_username

def get_select_box_kgtn(pconnection):
    query = "SELECT KGTN.Teilnehmer, Anmeldung_test.first_name, Anmeldung_test.last_name FROM `KGTN` INNER JOIN Anmeldung_test ON Anmeldung_test.id=KGTN.Teilnehmer;"
    df_joined = pd.read_sql(query, pconnection, "Teilnehmer")
    select_box_values = pd.DataFrame({
        "Name": df_joined["first_name"] + " " + df_joined["last_name"]
    }, index=df_joined.index.values)
    select_box_values = pd.concat([pd.DataFrame(["---"], columns=select_box_values.columns), select_box_values])
    #select_box_values.sort_values("Name", inplace=True)
    return select_box_values

def get_select_box_kg(pconnection):
    query = "SELECT Kleingruppen.id, Anmeldung_test.first_name, Anmeldung_test.last_name FROM Kleingruppen INNER JOIN KGTN ON Kleingruppen.leiter = KGTN.Teilnehmer INNER JOIN Anmeldung_test ON KGTN.Teilnehmer = Anmeldung_test.id"
    df_joined = pd.read_sql(query, pconnection, "id")
    select_box_values = pd.DataFrame({
        "name": "Kleingruppe von " + df_joined["first_name"] + " " + df_joined["last_name"]
    }, index=df_joined.index.values)
    select_box_values.sort_values("name", inplace=True)
    return select_box_values

def display_kg(pid):
    pass

################# Views #######################

def import_teilnehmer_view(pconnection):
    if st.button("Import Teilnehmer"):
        df_anmeldung = get_df_anmeldung(pconnection)
        df_kgtn = get_df_kgtn(pconnection)

        for anmeldung in df_anmeldung.iterrows():
            current_id = anmeldung[1]["id"]

            if not df_kgtn["Teilnehmer"].isin([current_id]).any():
            # Teilnehmer does not exist in KG-Liste
                query_create = f"INSERT INTO `KGTN` (`Teilnehmer`) VALUES ({current_id})"
                create_cursor = pconnection.cursor()
                create_cursor.execute(query_create)
                st.write(f"Kleingruppen Korrelation für {anmeldung[1]['first_name']} {anmeldung[1]['last_name']} wurde erstellt")

            pconnection.commit()
        st.write("Alle Änderungen erledigt")

def create_kleingruppe_view(pconnection):
    select_box_values = get_select_box_kgtn(pconnection)

    st.markdown("# Kleingruppe erstellen")
    with st.form("kg_create_form", clear_on_submit=True):
        leiter = st.selectbox("Kleingruppen Leiter", select_box_values.index.values, format_func=lambda x: select_box_values.loc[x,"Name"])
        coleiter = st.selectbox("Kleingruppen Co-Leiter", select_box_values.index.values, format_func=lambda x: select_box_values.loc[x,"Name"])
        username = st.text_input("Benutzername", value=get_new_username(pconnection))
        password = st.text_input("Passwort", value=get_random_password())

        if st.form_submit_button("Kleingruppe erstellen"):
            if leiter == 0:
                leiter = "NULL"

            if coleiter == 0:
                coleiter = "NULL"

            query = f"INSERT INTO `Kleingruppen` (leiter, coleiter, username, password) VALUES ({leiter}, {coleiter}, '{username}', '{password}')"
            cursor_create_kg = pconnection.cursor()
            cursor_create_kg.execute(query)
            pconnection.commit()
            st.write("Kleingruppe wurde erstellt")

def update_kleingruppe_view(pconnection):
    kg_select_data = get_select_box_kg(pconnection)
    selected_kleingruppe = st.sidebar.selectbox("Kleingruppe", kg_select_data.index.values, format_func=lambda x: kg_select_data.loc[x, "name"], index=0)
    select_box_values = get_select_box_kgtn(pconnection)

    st.markdown("# Kleingruppe updaten")
    kg_data = get_df_kleingruppen(pconnection)
    kgtn_data = get_df_kgtn(pconnection)

    current_kg = kg_data[kg_data["id"] == selected_kleingruppe].iloc[0,:]


    with st.form("kg_update_form"):
        if current_kg["leiter"] is None or np.isnan(current_kg["leiter"]):
            leiter = st.selectbox("Kleingruppen Leiter", select_box_values.index.values, format_func=lambda x: select_box_values.loc[x,"Name"], index=0)
        else:
            leiter_index = int(kgtn_data[current_kg["leiter"]==kgtn_data["Teilnehmer"]].index[0]) + 1
            leiter = st.selectbox("Kleingruppen Leiter", select_box_values.index.values, format_func=lambda x: select_box_values.loc[x,"Name"], index=leiter_index)

        if current_kg["coleiter"] is None or np.isnan(current_kg["coleiter"]):
            coleiter = st.selectbox("Kleingruppen Co-Leiter", select_box_values.index.values, format_func=lambda x: select_box_values.loc[x,"Name"], index=0)
        else:
            coleiter_index = int(kgtn_data[current_kg["coleiter"]==kgtn_data["Teilnehmer"]].index[0]) + 1
            coleiter = st.selectbox("Kleingruppen Co-Leiter", select_box_values.index.values, format_func=lambda x: select_box_values.loc[x,"Name"], index=coleiter_index)

        username = st.text_input("Benutzername", value=current_kg["username"])
        password = st.text_input("Passwort", value=current_kg["password"])

        if st.form_submit_button("Kleingruppe updaten"):
            if leiter == 0:
                leiter = "NULL"

            if coleiter == 0:
                coleiter = "NULL"

            query = f"UPDATE `Kleingruppen` SET leiter={leiter}, coleiter={coleiter}, username='{username}', password='{password}' WHERE id={current_kg['id']}"
            cursor_update_kg = pconnection.cursor()
            cursor_update_kg.execute(query)
            pconnection.commit()
            st.write("Kleingruppe wurde erstellt")

            st.experimental_rerun()

def assign_teilnehmer_to_kleingruppe_view(pconnection: mysql.connector.MySQLConnection):
    st.markdown("# Teilnehmer zuteilen")
    all_teilnehmer = get_df_kgtn(pconnection)
    all_kleingruppen = get_df_kleingruppen(pconnection)
    all_anmeldungen = get_df_anmeldung(pconnection)
    all_kleingruppen
    with st.form("assign_form") as assign_form:
        for current_tn  in all_teilnehmer.iterrows():
            col1, col2 = st.columns(2)
            current_tn_values = current_tn[1]
            tn_id = current_tn[1]["Teilnehmer"]
            kg_id = current_tn[1]["Kleingruppe"]
            if kg_id is None:
                kg_pos = 0
            else:
                # find position in kg
                kg_pos = 1

            # st.write(tn_id)
            tn = all_anmeldungen[all_anmeldungen["id"] == tn_id]
            tn = tn.iloc[0,:]
            name = "\n" + tn["first_name"] + " " +  tn["last_name"]
            col1.write(" ")
            col1.write(" ")
            col1.write(name)
            col2.selectbox("Kleingruppe", all_kleingruppen["id"], index=kg_pos, format_func=lambda x: all_anmeldungen[all_kleingruppen[all_kleingruppen["id"]==x].iloc[0,:]["leiter"]==all_anmeldungen["id"]].iloc[0,:]["first_name"] + " " + all_anmeldungen[all_kleingruppen[all_kleingruppen["id"]==x].iloc[0,:]["leiter"]==all_anmeldungen["id"]].iloc[0,:]["last_name"] + " Kleingruppe" , key=f"{tn_id}_kg_select")

        if st.form_submit_button("Speichern"):
            cursor = pconnection.cursor()
            for current_tn  in all_teilnehmer.iterrows():
                tn_id = current_tn[1]["Teilnehmer"]
                kg = st.session_state[f"{tn_id}_kg_select"]
                query = f"UPDATE KGTN SET `Kleingruppe`= {kg} WHERE `Teilnehmer`={tn_id}"
                cursor.execute(query)
                st.write(f"Updated Teilnehmer {tn_id}")

            pconnection.commit()

def kleingruppen_overview(pconnection):
    all_anmeldungen = get_df_anmeldung(pconnection)
    st.markdown("# Kleingruppen Anzeigen")
    kg_data = get_df_kleingruppen(pconnection)
    kgtn_data = get_df_kgtn(pconnection)

    for kg in kg_data.iterrows():
        kg_leiter_id = kg[1]["leiter"]
        kg_coleiter_id = kg[1]["coleiter"]

        if not (kg_leiter_id is None or np.isnan(kg_leiter_id)):
            kg_data.loc[kg[0], "leiter"] = all_anmeldungen[all_anmeldungen["id"]==kg_leiter_id].iloc[0,:]["first_name"] + " " + all_anmeldungen[all_anmeldungen["id"]==kg_leiter_id].iloc[0,:]["last_name"]

        if not (kg_coleiter_id is None or np.isnan(kg_coleiter_id)):
            kg_data.loc[kg[0], "coleiter"] = all_anmeldungen[all_anmeldungen["id"]==kg_coleiter_id].iloc[0,:]["first_name"] + " " + all_anmeldungen[all_anmeldungen["id"]==kg_coleiter_id].iloc[0,:]["last_name"]

    st.dataframe(kg_data)

def login_view(pconnection):
    with st.form("login"):
        username = st.text_input(label="Benutzername")
        password = st.text_input(label="Passwort", type="password", autocomplete="password")
        if st.form_submit_button("Einloggen"):
            if username == st.secrets["ADMIN_USER"] and password == st.secrets["ADMIN_PASSWORD"]:
                st.session_state["logged_in"] = "1"
                st.experimental_rerun()

try:
    logged_in_state = st.session_state["logged_in"]
except KeyError:
    logged_in_state = "0"

if logged_in_state == "1":
    views = {
        "Kleingruppe erstellen": create_kleingruppe_view,
        "Update Kleingruppe": update_kleingruppe_view,
        "Teilnehmer Kleingruppe zuordnen": assign_teilnehmer_to_kleingruppe_view,
        "Übersicht Kleingruppen": kleingruppen_overview
    }
else:
    views = {
        "Einloggen": login_view
    }

current_view = st.sidebar.radio("Ansicht", views)

views[current_view](connection)

# import_teilnehmer_view(connection)
# create_kleingruppe_view(connection)
# update_kleingruppe_view(connection)
#assign_teilnehmer_to_kleingruppe_view(connection)