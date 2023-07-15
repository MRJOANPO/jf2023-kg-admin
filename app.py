import streamlit as st
import pandas as pd
import mysql.connector

connection = mysql.connector.connect(
    host=st.secrets["DATABASE_HOST"],
    port=st.secrets["DATABASE_PORT"],
    user=st.secrets["DATABASE_USER"],
    password=st.secrets["DATABASE_PASSWORD"],
    database=st.secrets["DATABASE_NAME"]
)

try:
    current_kg = st.session_state["kg"]
except KeyError:
    current_kg = 0

if current_kg == 0:
    with st.form("login"):
        username = st.text_input(label="Benutzername")
        password = st.text_input(label="Passwort", type="password", autocomplete="password")
        if st.form_submit_button("Einloggen"):
            query = f"SELECT * FROM `Kleingruppen` WHERE `username`='{username}' AND `password`='{password}'"
            login_cursor = connection.cursor()
            login_cursor.execute(query)
            list_login = login_cursor.fetchall()
            if len(list_login) == 1:
                kg_id = list_login[0][0]
                st.session_state["kg"] = kg_id
                st.experimental_rerun()
            else:
                st.session_state["kg"] = 0
                st.write("Login nicht erfolgreich")
else:
    st.markdown("# Deine Kleingruppe")
    query = f"SELECT `Teilnehmer` FROM KGTN WHERE `Kleingruppe` = {st.session_state['kg']}"
    select_cursor = connection.cursor()
    select_cursor.execute(query)
    all_teilnehmer = select_cursor.fetchall()
    for teilnehmer in all_teilnehmer:
        teilnehmer_id = teilnehmer[0]
        query = f"SELECT `first_name`, `last_name`, `phone`, `gender`, `birthday`, `allergies`, `mental_issues`, `chronical_diseases`, `medication`, `zecken_impfung`, `tetanus_impfung`, `swim_confirm`, `leave_confirm` FROM `Anmeldung_test` WHERE `id`={teilnehmer_id}"
        if teilnehmer == all_teilnehmer[0]:
            tn_data = pd.read_sql(query, connection)
        else:
            tn_data = pd.concat([tn_data, pd.read_sql(query, connection)])

    tn_data["zecken_impfung"] = tn_data["zecken_impfung"] == 1
    tn_data["tetanus_impfung"] = tn_data["tetanus_impfung"] == 1
    tn_data["leave_confirm"] = tn_data["leave_confirm"] == 1
    tn_data["swim_confirm"] = tn_data["swim_confirm"] == 1
    tn_data.columns = ["Vorname", "Nachname", "Handynummer", "Geschlecht", "Geburtstag", "Allergien und Unverträglichkeiten", "Geistige oder soziale Beeinträchtigungen", "Chronische Erkrankungen", "Regelmäßige Medikamenteneinnahme", "Zeckenimpfung", "Tetanusimpfung", "Das Schwimmen gehen", "Darf das Gelände verlassen"]
    st.write(tn_data)