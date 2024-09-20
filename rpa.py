import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

conn_db1 = psycopg2.connect(os.getenv('DB1_URL'))
conn_db2 = psycopg2.connect(os.getenv('DB2_URL'))

cursor_db1 = conn_db1.cursor()
cursor_db2 = conn_db2.cursor()



cursor_bd1.execute("SELECT ID, nome, descricao, endereco, acessibilidade FROM atracao")
atracoes_bd1 = cursor_bd1.fetchall()

for atracao in atracoes_bd1:
    id_atracao, nome, descricao, endereco, acessibilidade = atracao

    cursor_bd2.execute("SELECT ID FROM atracao WHERE ID = %s", (id_atracao,))
    atracao_bd2 = cursor_bd2.fetchone()

    if atracao_bd2:
        cursor_bd2.execute("""
            UPDATE atracao
            SET nome = %s, descricao = %s, endereco = %s, acessibilidade = %s
            WHERE ID = %s
        """, (nome, descricao, endereco, acessibilidade, id_atracao))

        cursor_bd2.execute("""
            INSERT INTO log_atracao (id_atracao, operacao, data_operacao)
            VALUES (%s, %s, %s)
        """, (id_atracao, 'UPDATE', datetime.now()))
    
    else:
        cursor_bd2.execute("""
            INSERT INTO atracao (ID, nome, descricao, endereco, acessibilidade)
            VALUES (%s, %s, %s, %s, %s)
        """, (id_atracao, nome, descricao, endereco, acessibilidade))

        cursor_bd2.execute("""
            INSERT INTO log_atracao (id_atracao, operacao, data_operacao)
            VALUES (%s, %s, %s)
        """, (id_atracao, 'INSERT', datetime.now()))

conn_bd2.commit()
cursor_bd1.close()
conn_bd1.close()
cursor_bd2.close()
conn_bd2.close()
