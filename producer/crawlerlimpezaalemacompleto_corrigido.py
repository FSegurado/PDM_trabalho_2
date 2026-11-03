import pandas as pd
import soccerdata as sd

def extrair_e_limpar_fbref(seasons=["2627"], output_path="players_stats_final.csv"):
    # 1. Extrair dados via soccerdata (mantém o processo oculto de navegação)
    print(f"[*] A extrair dados da(s) temporada(s) {seasons}...")
    fbref = sd.FBref(leagues="Big 5 European Leagues Combined", seasons=seasons)
    df_raw = fbref.read_player_season_stats(stat_type="standard").reset_index()
    print(f"[*] Foram extraídos {len(df_raw)} registos.")

    # 2. Achatar o MultiIndex das colunas num formato limpo
    # O reset_index() transforma as chaves (league, season, team, player) em tuplos ('league', '')
    novas_colunas = []
    for col in df_raw.columns:
        if isinstance(col, tuple):
            grupo, metrica = col[0].strip(), str(col[1]).strip()
            if metrica == "":
                # chaves (league, season, team, player): sem sufixo
                novas_colunas.append(grupo)
            elif grupo == "" or grupo.startswith("Unnamed"):
                # colunas sem grupo (nation, pos, age, born, ...): sufixo "_" para
                # bater com o esquema usado pelo resto do pipeline (ballon_common/
                # preprocessing.py e os dados do Trabalho 1 esperam "nation_",
                # "pos_", "age_", "born_" — sem o "_" esses campos chegam nulos
                # na Silver, mesmo sem dar erro)
                novas_colunas.append(f"{metrica}_")
            else:
                novas_colunas.append(f"{grupo}_{metrica}")
        else:
            novas_colunas.append(col)

    df_clean = df_raw.copy()
    df_clean.columns = novas_colunas

    # Garantir nomes padronizados para as colunas de identificação
    rename_map = {"league": "league", "season": "season", "team": "team", "player": "player"}
    df_clean = df_clean.rename(columns=rename_map)

    # 3. Corrigir a liga alemã (Bundesliga) que o soccerdata perde
    bundesliga_teams = [
        "Augsburg", "Bayern Munich", "Dortmund", "Frankfurt", "Freiburg",
        "Gladbach", "Hamburger SV", "Heidenheim", "Hoffenheim", "Köln",
        "Leverkusen", "Mainz 05", "RB Leipzig", "St Pauli", "Stuttgart",
        "Union Berlin", "Werder Bremen", "Wolfsburg", "Bochum", "Darmstadt",
        "Kiel", "St. Pauli", "Bremen"
    ]

    mask = df_clean["league"].isna() & df_clean["team"].isin(bundesliga_teams)
    df_clean.loc[mask, "league"] = "GER-Bundesliga"

    # 4. Preenchimento (forward fill) para qualquer lacuna de liga restante
    df_clean["league"] = df_clean["league"].ffill()

    # 5. Guardar o ficheiro final com separador ';'
    df_clean.to_csv(output_path, sep=";", index=False)
    print(f"[+] Sucesso! Ficheiro final unificado guardado como '{output_path}'.")
    print("Ligas finais presentes no ficheiro:", df_clean["league"].unique())

if __name__ == "__main__":
    extrair_e_limpar_fbref()
