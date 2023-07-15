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

def get_tn_data(all_teilnehmer, all_anmeldungen):
    tn_data = pd.DataFrame()
    for teilnehmer in all_teilnehmer.iterrows():
        tn_id = teilnehmer[1]["Teilnehmer"]
        kg_id  = teilnehmer[1]["Kleingruppe"]
        tn = all_anmeldungen[all_anmeldungen["id"] == tn_id].iloc[0,:]
        tn_name = tn["first_name"] + " " + tn["last_name"]

        current_tn_data = {
            "teilnehmer_id": tn_id,
            "kleingruppe_id": kg_id,
            "teilnehmer_name": tn_name,
            "birthday": tn["birthday"],
            "gender": tn["gender"]
        }
        tn_data = pd.concat([tn_data, pd.DataFrame(current_tn_data, index=[0])])

    tn_data.sort_values(["gender", "birthday"], ascending=[True, False], inplace=True)
    tn_data.reset_index(inplace=True, drop=True)
    return tn_data

def get_kg_data(all_kleingruppen, all_anmeldungen):
    kg_data = pd.DataFrame()

    for kleingruppe in all_kleingruppen.iterrows():
        kg_id = kleingruppe[1]["id"]
        kg_leiter_id = kleingruppe[1]["leiter"]
        kg_coleiter_id = kleingruppe[1]["coleiter"]
        if (kg_leiter_id is None) or np.isnan(kg_leiter_id):
            kg_leiter_id = 0
            leiter_name = "<Kein Leiter>"
        else:
            leiter = all_anmeldungen[all_anmeldungen["id"]==kg_leiter_id].iloc[0,:]
            leiter_name = leiter["first_name"] + " " + leiter["last_name"]

        if (kg_coleiter_id is None) or np.isnan(kg_coleiter_id):
            kg_coleiter_id = 0
            coleiter_name = ""
        else:
            coleiter = all_anmeldungen[all_anmeldungen["id"]==kg_coleiter_id].iloc[0,:]
            coleiter_name = coleiter["first_name"] + " " + coleiter["last_name"]

        kg_display_name = "KG von " + leiter_name

        current_kg_data = {
            "kg_id": kg_id,
            "kg_leiter_id": kg_leiter_id,
            "kg_coleiter_id": kg_coleiter_id,
            "kg_display_name": kg_display_name,
            "kg_leiter_name": leiter_name,
            "kg_coleiter_name": coleiter_name,
            "username":kleingruppe[1]["username"],
            "password":kleingruppe[1]["password"]
        }
        kg_data = pd.concat([kg_data, pd.DataFrame(current_kg_data, index=[0])])

    kg_data.sort_values(["kg_id"], inplace=True)
    kg_data = pd.concat([
        pd.DataFrame({
            "kg_id": 0,
            "kg_leiter_id": 0,
            "kg_display_name": "---"
        }, index=[0]),
        kg_data
    ])

    kg_data.set_index("kg_id", inplace=True, drop=True)
    return kg_data

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
    st.markdown("# Kleingruppe updaten")

    all_kleingruppen = get_df_kleingruppen(pconnection)
    all_anmeldungen = get_df_anmeldung(pconnection)
    all_teilnehmer = get_df_kgtn(pconnection)
    kg_data = get_kg_data(all_kleingruppen, all_anmeldungen)
    tn_data = get_tn_data(all_teilnehmer, all_anmeldungen)
    kg_data.drop([0], inplace=True)
    selected_kleingruppe = st.sidebar.selectbox("Kleingruppe", kg_data.index, format_func=lambda x: kg_data.loc[x, "kg_display_name"], index=0)

    current_kg = kg_data[kg_data.index == selected_kleingruppe].iloc[0,:]

    kg_data.reset_index()
    tn_data = pd.concat([
        pd.DataFrame({
            "teilnehmer_name": "---",
            "teilnehmer_id": 0
        }, index=[0]),
        tn_data
    ])
    tn_data.reset_index(drop=True, inplace=True)

    with st.form("kg_update_form"):

        current_leiter_index = int(tn_data[tn_data["teilnehmer_id"]==current_kg["kg_leiter_id"]].index[0])
        current_coleiter_index = int(tn_data[tn_data["teilnehmer_id"]==current_kg["kg_coleiter_id"]].index[0])

        leiter_index = st.selectbox("Kleingruppen Leiter", tn_data.index.astype(int), format_func=lambda x: tn_data.loc[x,"teilnehmer_name"], index=current_leiter_index)
        coleiter_index = st.selectbox("Kleingruppen Co-Leiter", tn_data.index.astype(int), format_func=lambda x: tn_data.loc[x,"teilnehmer_name"], index=current_coleiter_index)


        username = st.text_input("Benutzername", value=current_kg["username"])
        password = st.text_input("Passwort", value=current_kg["password"])

        if st.form_submit_button("Kleingruppe updaten"):
            if leiter_index == 0:
                leiter = "NULL"
            else:
                leiter = int(tn_data.loc[leiter_index, "teilnehmer_id"])

            if coleiter_index == 0:
                coleiter = "NULL"
            else:
                coleiter = int(tn_data.loc[coleiter_index, "teilnehmer_id"])
            query = f"UPDATE `Kleingruppen` SET leiter={leiter}, coleiter={coleiter}, username='{username}', password='{password}' WHERE id={current_kg.name}"
            cursor_update_kg = pconnection.cursor()
            cursor_update_kg.execute(query)
            pconnection.commit()
            st.write("Kleingruppe wurde aktualisiert")

def assign_teilnehmer_to_kleingruppe_view(pconnection: mysql.connector.MySQLConnection):
    st.markdown("# Teilnehmer zuteilen")
    all_teilnehmer = get_df_kgtn(pconnection)
    all_kleingruppen = get_df_kleingruppen(pconnection)
    all_anmeldungen = get_df_anmeldung(pconnection)
    kg_data = get_kg_data(all_kleingruppen, all_anmeldungen)
    tn_data = get_tn_data(all_teilnehmer, all_anmeldungen)

    with st.form("assign_form"):
        for current_tn  in tn_data.iterrows():
            col1, col2 = st.columns(2)
            current_tn_values = current_tn[1]
            kg_id = current_tn_values["kleingruppe_id"]

            if (kg_id is None) or (np.isnan(kg_id)):
                kg_pos = 0
            else:
                # find position in kg
                kg_pos_data = kg_data.reset_index()
                kg_pos = int(kg_pos_data[kg_pos_data["kg_id"]==kg_id].index[0])

            col1.write(" ")
            col1.write(" ")
            col1.write(current_tn_values["teilnehmer_name"])
            col2.selectbox("Kleingruppe", kg_data.index, index=kg_pos, format_func=lambda x: kg_data.loc[x,"kg_display_name"], key=f"{current_tn_values['teilnehmer_id']}_kg_select")

        if st.form_submit_button("Speichern"):
            cursor = pconnection.cursor()
            for current_tn  in tn_data.iterrows():
                tn_id = current_tn[1]["teilnehmer_id"]
                kg_id = st.session_state[f"{tn_id}_kg_select"]
                if kg_id != 0:
                    query = f"UPDATE KGTN SET `Kleingruppe`= {kg_id} WHERE `Teilnehmer`={tn_id}"
                else:
                    query = f"UPDATE KGTN SET `Kleingruppe`=NULL WHERE `Teilnehmer`={tn_id}"
                cursor.execute(query)
                st.write(f"Updated Teilnehmer {current_tn[1]['teilnehmer_name']}")

            pconnection.commit()

def kleingruppen_overview(pconnection):

    st.markdown("# Kleingruppen Anzeigen")
    all_teilnehmer = get_df_kgtn(pconnection)
    all_kleingruppen = get_df_kleingruppen(pconnection)
    all_anmeldungen = get_df_anmeldung(pconnection)
    kg_data = get_kg_data(all_kleingruppen, all_anmeldungen)
    tn_data = get_tn_data(all_teilnehmer, all_anmeldungen)
    kg_data.drop([0], inplace=True)
    kg_data.drop(columns=["kg_leiter_id", "kg_coleiter_id", "kg_display_name"], inplace=True)
    st.dataframe(kg_data)

    for kg in kg_data.iterrows():
        st.markdown(f"#### Kleingruppe von {kg[1]['kg_leiter_name']}")
        teilnehmer = tn_data[tn_data["kleingruppe_id"] == kg[0]]
        teilnehmer.reset_index(inplace=True)
        for tn in teilnehmer.iterrows():
            st.markdown(f"{tn[0]+1}. {tn[1]['teilnehmer_name']}")

        if len(teilnehmer) == 0:
            st.markdown("*noch keine Teilnehmer*")

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