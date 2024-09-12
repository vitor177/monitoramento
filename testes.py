import sqlite3
from datetime import datetime, date

conn = sqlite3.connect('banco_de_dados_vitinho.db')

# Cursor para executar comandos SQL
cursor = conn.cursor()

timestamp_to_filter = '"2024-07-17 12:05:49"'

# Consulta os dados da tabela
#cursor.execute(f"SELECT * FROM dados WHERE TIMESTAMP = '{timestamp_to_filter}'")# Recupera e imprime todos os resultados da consulta

# TABELAS ATUALIZADAS A CADA SEGUNDO
# ('SAO_JOAO_PIAUI__PI_GHI_seg_SJP_PI',)
# ('ILHA_SOLTEIRA_SP_GHI_seg_ILHA_SP',)
# ('SOUSA_PB_GHI_seg_SOUSA_PB',)
# ('PIRASSUNUNGA_SP_Tabela_seg_ES02',)
#cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

cursor.execute("SELECT COUNT(*) FROM SAO_JOAO_PIAUI__PI_GHI_seg_SJP_PI")

#cursor.execute("SELECT * FROM SAO_JOAO_PIAUI__PI_GHI_seg_SJP_PI ORDER BY TIMESTAMP DESC limit 10")


#cursor.execute("SELECT * FROM PIRASSUNUNGA_SP_Tabela_seg_ES02 ORDER BY TIMESTAMP ASC LIMIT 3")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()

# PIRASSUNUNGA = "TIMESTAMP","RECORD","GHI1","DHI","BNI"

# SÃO JOÃO PIUAI = "TIMESTAMP","RECORD","GHI1","GHI2","GHI3","GRI","Cell_Isc"

# ILHA SOLTEIRA = "TIMESTAMP","RECORD","GHI1","GHI2","GHI3","GRI","Cell_Isc"

# SOUSA = "TIMESTAMP","RECORD","GHI1","GHI2","GHI3","GRI","Cell_Isc"


