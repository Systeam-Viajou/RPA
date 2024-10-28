import psycopg2
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

try:
    conn_db1 = psycopg2.connect(os.getenv('POSTGRE1_URL'))
    print("Conexão com DB1 estabelecida.")
except Exception as e:
    print(f"Erro ao conectar ao DB1: {e}")

try:
    conn_db2 = psycopg2.connect(os.getenv('POSTGRE2_URL'))
    print("Conexão com DB2 estabelecida.")

except Exception as e:
    print(f"Erro ao conectar ao DB2: {e}")

try:
    client = MongoClient(os.getenv('MONGO_HOST'))
    db_mongo = client[os.getenv('MONGO_DB')]
    colecao_imagem = db_mongo['imagem']
    print("Conexão com MongoDB2 estabelecida.")

except Exception as e:
    print(f"Erro ao conectar ao MongoDB: {e}")

cursor_db1 = conn_db1.cursor()
cursor_db2 = conn_db2.cursor()

# Contadores para logs finais
contadores = {
    "PLANO": {"update": 0, "insert": 0, "delete": 0},
    "IMAGEM": {"update": 0, "insert": 0, "delete": 0},
    "ATRACAO": {"update": 0, "insert": 0, "delete": 0},
    "EVENTO": {"update": 0, "insert": 0, "delete": 0},
    "EXCURSAO": {"update": 0, "insert": 0, "delete": 0},
    "PONTO_TURISTICO": {"update": 0, "insert": 0, "delete": 0}
}

# ----- PLANO -----
cursor_db1.execute("SELECT ID, nome, descricao, livre_propaganda, preco, data_criacao, data_atualizacao, duracao FROM plano")
planos_bd1 = cursor_db1.fetchall()

for plano in planos_bd1:
    id_plano, nome, descricao, livre_propaganda, preco, data_criacao, data_atualizacao, duracao = plano
    cursor_db2.execute("SELECT nome, descricao, livre_propaganda, valor, duracao, data_desativacao FROM plano WHERE ID = %s", (id_plano,))
    plano_bd2 = cursor_db2.fetchone()

    if plano_bd2:
        if data_atualizacao is not None and plano_bd2[5] is None:
            cursor_db2.execute("""
                UPDATE plano
                SET nome = %s, descricao = %s, livre_propaganda = %s, valor = %s, duracao = %s
                WHERE ID = %s
            """, (nome, descricao, livre_propaganda, preco.replace('$', '').replace(',', ''), duracao, id_plano))
            conn_db2.commit()
            contadores["PLANO"]["update"] += 1
    else:
        if data_criacao is not None:
            preco_formatado = float(preco.replace('$', '').replace(',', ''))
            cursor_db2.execute("""
                INSERT INTO plano (ID, nome, descricao, livre_propaganda, valor, duracao)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (id_plano, nome, descricao, livre_propaganda, preco_formatado, duracao))
            conn_db2.commit()
            contadores["PLANO"]["insert"] += 1

# Desativação de planos
cursor_db2.execute("SELECT ID FROM plano")
ids_db2 = {plano[0] for plano in cursor_db2.fetchall()}
ids_db1 = {plano[0] for plano in planos_bd1}
ids_para_desativar = ids_db2 - ids_db1

for id_del in ids_para_desativar:
    cursor_db2.execute("UPDATE plano SET data_desativacao = NOW() WHERE ID = %s AND data_desativacao IS NULL", (id_del,))
    conn_db2.commit()
    contadores["PLANO"]["delete"] += 1

# ----- IMAGEM -----
cursor_db1.execute("SELECT url, ID_atracao FROM imagem")
imagens_bd1 = cursor_db1.fetchall()
imagens_atuais_db1 = {(img[0], img[1]) for img in imagens_bd1}

for imagem in imagens_bd1:
    url, id_atracao = imagem
    imagem_existente = colecao_imagem.find_one({"url": url, "id_atracao": id_atracao})

    if not imagem_existente:
        nova_imagem = {"url": url, "id_atracao": id_atracao}
        colecao_imagem.insert_one(nova_imagem)
        contadores["IMAGEM"]["insert"] += 1
    else:
        if imagem_existente['url'] != url or imagem_existente['id_atracao'] != id_atracao:
            colecao_imagem.update_one({"url": url, "id_atracao": id_atracao}, {"$set": {"url": url, "id_atracao": id_atracao}})
            contadores["IMAGEM"]["update"] += 1

imagens_mongo = colecao_imagem.find({})
for imagem_mongo in imagens_mongo:
    url = imagem_mongo['url']
    id_atracao = imagem_mongo['id_atracao']
    if (url, id_atracao) not in imagens_atuais_db1:
        colecao_imagem.delete_one({"url": url, "id_atracao": id_atracao})
        contadores["IMAGEM"]["delete"] += 1

# ----- ATRACAO -----
cursor_db1.execute("SELECT ID, descricao, nome, endereco, acessibilidade, categoria, data_criacao, data_atualizacao FROM atracao")
atracoes_bd1 = cursor_db1.fetchall()

for atracao in atracoes_bd1:
    id_atracao, descricao, nome, endereco, acessibilidade, categoria, data_criacao, data_atualizacao = atracao
    cursor_db2.execute("SELECT ID FROM categoria WHERE nome = %s", (categoria,))
    categoria_bd2 = cursor_db2.fetchone()

    if not categoria_bd2:
        cursor_db2.execute("INSERT INTO categoria (nome) VALUES (%s) RETURNING ID", (categoria,))
        categoria_id = cursor_db2.fetchone()[0]
        conn_db2.commit()
    else:
        categoria_id = categoria_bd2[0]

    cursor_db2.execute("SELECT ID FROM atracao WHERE ID = %s", (id_atracao,))
    atracao_bd2 = cursor_db2.fetchone()

    if atracao_bd2:
        cursor_db2.execute("""
            UPDATE atracao
            SET nome = %s, descricao = %s, endereco = %s, acessibilidade = %s, ID_categoria = %s
            WHERE ID = %s
        """, (nome, descricao, endereco, acessibilidade, categoria_id, id_atracao))
        conn_db2.commit()
        contadores["ATRACAO"]["update"] += 1
    else:
        cursor_db2.execute("""
            INSERT INT......................O atracao (ID, nome, descricao, endereco, acessibilidade, ID_categoria)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (id_atracao, nome, descricao, endereco, acessibilidade, categoria_id))
        conn_db2.commit()
        contadores["ATRACAO"]["insert"] += 1

# ----- EVENTO -----
cursor_db1.execute("SELECT ID, data_inicio, preco_pessoa, ID_atracao FROM eventos")
eventos_bd1 = cursor_db1.fetchall()

for evento in eventos_bd1:
    id_evento, data_inicio, preco_pessoa, id_atracao = evento
    preco_pessoa_float = float(preco_pessoa.replace('$', '').replace(',', '').strip())
    cursor_db2.execute("SELECT ID FROM evento WHERE ID = %s", (id_evento,))
    evento_bd2 = cursor_db2.fetchone()

    if evento_bd2:
        cursor_db2.execute("""
            UPDATE evento
            SET data_inicio = %s, preco_pessoa = %s, ID_atracao = %s, data_desativacao = NULL
            WHERE ID = %s
        """, (data_inicio, preco_pessoa_float, id_atracao, id_evento))
        conn_db2.commit()
        contadores["EVENTO"]["update"] += 1
    else:
        cursor_db2.execute("""
            INSERT INTO evento (ID, data_inicio, preco_pessoa, ID_atracao)
            VALUES (%s, %s, %s, %s)
        """, (id_evento, data_inicio, preco_pessoa_float, id_atracao))
        conn_db2.commit()
        contadores["EVENTO"]["insert"] += 1

# ----- EXCURSAO -----
cursor_db1.execute("SELECT ID, nome_empresa, capacidade, duracao, site, preco_total, data_inicio, data_termino, ID_atracao FROM excursao")
excursao_bd1 = cursor_db1.fetchall()

for excursao in excursao_bd1:
    id_excursao, nome_empresa, capacidade, duracao, site, preco_total, data_inicio, data_termino, id_atracao = excursao
    preco_total_float = float(preco_total.replace('$', '').replace(',', ''))
    cursor_db2.execute("SELECT ID FROM empresa WHERE nome = %s", (nome_empresa,))
    empresa_bd2 = cursor_db2.fetchone()

    if not empresa_bd2:
        cursor_db2.execute("INSERT INTO empresa (nome, site_empresa) VALUES (%s, %s) RETURNING ID", (nome_empresa, site))
        id_empresa = cursor_db2.fetchone()[0]
        conn_db2.commit()
    else:
        id_empresa = empresa_bd2[0]

    cursor_db2.execute("SELECT ID FROM excursao WHERE ID = %s", (id_excursao,))
    excursao_bd2 = cursor_db2.fetchone()

    if excursao_bd2:
        cursor_db2.execute("""
            UPDATE excursao
            SET capacidade = %s, qntd_pessoas = %s, preco_total = %s, data_inicio = %s, data_termino = %s, ID_empresa = %s
            WHERE ID = %s
        """, (capacidade, duracao, preco_total_float, data_inicio, data_termino, id_empresa, id_excursao))
        conn_db2.commit()
        contadores["EXCURSAO"]["update"] += 1
    else:
        cursor_db2.execute("""
            INSERT INTO excursao (ID, capacidade, qntd_pessoas, preco_total, data_inicio, data_termino, ID_atracao, ID_empresa)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_excursao, capacidade, duracao, preco_total_float, data_inicio, data_termino, id_atracao, id_empresa))
        conn_db2.commit()
        contadores["EXCURSAO"]["insert"] += 1

# ----- PONTO TURISTICO -----
cursor_db1.execute("SELECT ID, ID_atracao, capacidade, preco_entrada, data_criacao, data_atualizacao FROM pontos_turisticos")
pontos_bd1 = cursor_db1.fetchall()

for ponto in pontos_bd1:
    id_ponto, id_atracao, capacidade, preco_entrada, data_criacao, data_atualizacao = ponto
    cursor_db2.execute("SELECT ID FROM ponto_turistico WHERE ID = %s", (id_ponto,))
    ponto_bd2 = cursor_db2.fetchone()

    if ponto_bd2:
        cursor_db2.execute("UPDATE ponto_turistico SET data_atualizacao = %s WHERE ID = %s", (data_atualizacao, id_ponto))
        conn_db2.commit()
        contadores["PONTO_TURISTICO"]["update"] += 1
    else:
        cursor_db2.execute("INSERT INTO ponto_turistico (ID, ID_atracao, data_desativacao) VALUES (%s, %s, NULL)", (id_ponto, id_atracao))
        conn_db2.commit()
        contadores["PONTO_TURISTICO"]["insert"] += 1

for tabela, operacoes in contadores.items():
    print(f"{tabela}: {operacoes['update']} atualizados, {operacoes['insert']} inseridos, {operacoes['delete']} desativados/deletados.")

# -=-=-=-=-=-=TOUR VIRTUAL=-=-=-=-=-=-=-

cursor_db1.close()
cursor_db2.close()
conn_db1.close()
conn_db2.close()