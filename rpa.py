import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

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

cursor_db1 = conn_db1.cursor()
cursor_db2 = conn_db2.cursor()

# #-----PLANO-----
# # Seleciona todos os planos de DB1 para sincronização
# cursor_db1.execute("""
#     SELECT ID, nome, descricao, livre_propaganda, preco, data_criacao, data_atualizacao, duracao
#     FROM plano
# """)
# planos_bd1 = cursor_db1.fetchall()

# # Verifica se há planos disponíveis em DB1
# if not planos_bd1:
#     print("Nenhum plano encontrado em DB1.")
# else:
#     print(f"{len(planos_bd1)} planos encontrados em DB1 para processamento.")

# # Processa cada plano encontrado em DB1
# for plano in planos_bd1:
#     id_plano, nome, descricao, livre_propaganda, preco, data_criacao, data_atualizacao, duracao = plano
#     print(f"Iniciando processamento do plano ID {id_plano} de DB1.")

#     # Verifica se o plano já existe em DB2
#     cursor_db2.execute("""
#         SELECT nome, descricao, livre_propaganda, valor, duracao
#         FROM plano WHERE ID = %s
#     """, (id_plano,))
#     plano_bd2 = cursor_db2.fetchone()

#     if plano_bd2:
#         # Atualiza o plano em DB2 se houver mudanças
#         print(f"Plano ID {id_plano} encontrado em DB2. Verificando necessidade de atualização.")
#         if data_atualizacao is not None:
#             cursor_db2.execute("""
#                 UPDATE plano
#                 SET nome = %s, descricao = %s, livre_propaganda = %s, valor = %s, duracao = %s
#                 WHERE ID = %s
#             """, (nome, descricao, livre_propaganda, preco.replace('$', '').replace(',', ''), duracao, id_plano))
#             conn_db2.commit()
#             print(f"Plano ID {id_plano} atualizado em DB2.")
#     else:
#         # Insere novo plano em DB2 se não encontrado
#         print(f"Plano ID {id_plano} não encontrado em DB2. Preparando para inserção.")
#         if data_criacao is not None:
#             preco_formatado = float(preco.replace('$', '').replace(',', ''))
#             cursor_db2.execute("""
#                 INSERT INTO plano (ID, nome, descricao, livre_propaganda, valor, duracao)
#                 VALUES (%s, %s, %s, %s, %s, %s)
#             """, (id_plano, nome, descricao, livre_propaganda, preco_formatado, duracao))
#             conn_db2.commit()
#             print(f"Plano ID {id_plano} inserido com sucesso em DB2.")
#         else:
#             print(f"A inserção do plano ID {id_plano} foi abortada devido à falta da data de criação.")

# # Identificação de planos em DB2 que precisam ser deletados
# cursor_db2.execute("SELECT ID FROM plano")
# ids_db2 = {plano[0] for plano in cursor_db2.fetchall()}
# ids_db1 = {plano[0] for plano in planos_bd1}
# ids_para_deletar = ids_db2 - ids_db1

# # Deleção de planos em DB2 que não existem mais em DB1
# for id_del in ids_para_deletar:
#     cursor_db2.execute("DELETE FROM plano WHERE ID = %s", (id_del,))
#     conn_db2.commit()
#     print(f"Plano ID {id_del} removido de DB2, pois não está mais presente em DB1.")

# -----ATRACAO-----
# Selecio# Seleciona todas as atrações de DB1 para sincronização
# Seleciona todas as atrações de DB1
cursor_db1.execute("""
    SELECT ID, descricao, nome, endereco, acessibilidade, categoria, data_criacao, data_atualizacao
    FROM atracao
""")
atracoes_bd1 = cursor_db1.fetchall()

# Verifica se há atrações disponíveis em DB1
if not atracoes_bd1:
    print("Nenhuma atração encontrada em DB1.")
else:
    print(f"{len(atracoes_bd1)} atrações encontradas em DB1 para processamento.")

# Processa cada atração encontrada em DB1
for atracao in atracoes_bd1:
    id_atracao, descricao, nome, endereco, acessibilidade, categoria, data_criacao, data_atualizacao = atracao

    # Verifica se a categoria da atração já existe em DB2 e insere se necessário
    cursor_db2.execute("""
        SELECT ID FROM categoria WHERE nome = %s
    """, (categoria,))
    categoria_bd2 = cursor_db2.fetchone()

    if not categoria_bd2:
        cursor_db2.execute("""
            INSERT INTO categoria (nome) VALUES (%s) RETURNING ID
        """, (categoria,))
        categoria_id = cursor_db2.fetchone()[0]
        conn_db2.commit()
        print(f"Categoria '{categoria}' inserida no DB2 com ID {categoria_id}.")
    else:
        categoria_id = categoria_bd2[0]
        print(f"Categoria '{categoria}' já existe no DB2 com ID {categoria_id}.")

    # Verifica se a atração já existe em DB2
    cursor_db2.execute("""
        SELECT ID, nome, descricao, endereco, acessibilidade, media_classificacao 
        FROM atracao WHERE ID = %s
    """, (id_atracao,))
    atracao_bd2 = cursor_db2.fetchone()

    if atracao_bd2 and data_atualizacao is not None:
        # Atualiza a atração em DB2 se houver mudanças
        print(f"Atração com ID {id_atracao} encontrada no DB2. Verificando necessidade de atualização.")
        cursor_db2.execute("""
            UPDATE atracao
            SET nome = %s, descricao = %s, endereco = %s, acessibilidade = %s, ID_categoria = %s
            WHERE ID = %s
        """, (nome, descricao, endereco, acessibilidade, categoria_id, id_atracao))
        conn_db2.commit()
        print(f"Atração com ID {id_atracao} atualizada em DB2.")
    elif not atracao_bd2:
        # Insere nova atração em DB2 se não encontrada
        print(f"Atração com ID {id_atracao} não encontrada em DB2. Preparando para inserção.")
        if data_criacao is not None:
            cursor_db2.execute("""
                INSERT INTO atracao (ID, nome, descricao, endereco, acessibilidade, ID_categoria)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (id_atracao, nome, descricao, endereco, acessibilidade, categoria_id))
            conn_db2.commit()
            print(f"Atração com ID {id_atracao} inserida com sucesso em DB2.")
        else:
            print(f"Atração com ID {id_atracao} não pode ser inserida porque a data de criação é nula.")

# Obter todos os IDs de atrações de db2
cursor_db2.execute("SELECT ID FROM atracao")
ids_db2 = {atracao[0] for atracao in cursor_db2.fetchall()}

# Utilizar os IDs coletados de db1 durante a inserção/atualização
ids_db1 = {atracao[0] for atracao in atracoes_bd1}

# Identificar quais IDs precisam ser deletados de db2
ids_para_deletar = ids_db2 - ids_db1

# Deletar as atrações não presentes em db1
for id_del in ids_para_deletar:
    cursor_db2.execute("DELETE FROM atracao WHERE ID = %s", (id_del,))
    conn_db2.commit()
    print(f"Atração com ID {id_del} removida de DB2.")

# Verificar categorias que ficaram órfãs após a exclusão das atrações
cursor_db2.execute("""
    SELECT c.ID
    FROM categoria c
    LEFT JOIN atracao a ON c.ID = a.ID_categoria
    WHERE a.ID_categoria IS NULL
""")
categorias_orfas = {categoria[0] for categoria in cursor_db2.fetchall()}

# Deletar categorias órfãs
for cat_id in categorias_orfas:
    cursor_db2.execute("DELETE FROM categoria WHERE ID = %s", (cat_id,))
    conn_db2.commit()
    print(f"Categoria com ID {cat_id} removida de DB2, pois ficou órfã.")

# # EVENTOS
# cursor_db1.execute("""
#     SELECT ID, data_inicio, preco_pessoa, ID_atracao
#     FROM eventos
# """)
# eventos_bd1 = cursor_db1.fetchall()

# if not eventos_bd1:
#     print("Nenhum evento encontrado em DB1.")
# else:
#     print(f"{len(eventos_bd1)} eventos encontrados em DB1.")

# for evento in eventos_bd1:
#     id_evento, data_inicio, preco_pessoa, id_atracao = evento

#     print(f"Processando evento ID {id_evento} do DB1.")

#     cursor_db2.execute("""
#         SELECT ID, data_inicio, preco_pessoa, ID_atracao
#         FROM evento WHERE ID = %s
#     """, (id_evento,))
#     evento_bd2 = cursor_db2.fetchone()

#     if evento_bd2:
#         print(f"Evento com ID {id_evento} encontrado no DB2.")
        
#         data_inicio_bd2, preco_pessoa_bd2, id_atracao_bd2 = evento_bd2[1], evento_bd2[2], evento_bd2[3]
#         preco_pessoa_float = float(preco_pessoa.replace('$', '').replace(',', ''))

#         if data_inicio != data_inicio_bd2 or preco_pessoa_float != preco_pessoa_bd2 or id_atracao != id_atracao_bd2:
#             cursor_db2.execute("""
#                 UPDATE evento
#                 SET data_inicio = %s, preco_pessoa = %s, ID_atracao = %s
#                 WHERE ID = %s
#             """, (data_inicio, preco_pessoa_float, id_atracao, id_evento))

#             conn_db2.commit()
#             print(f"Evento com ID {id_evento} atualizado no DB2")
#     else:
#         print(f"Evento com ID {id_evento} não encontrado no DB2. Preparando para inserir:")
        
#         if data_inicio is not None and preco_pessoa is not None:
#             preco_pessoa_float = float(preco_pessoa.replace('$', '').replace(',', ''))

#             cursor_db2.execute("""
#                 INSERT INTO evento (ID, data_inicio, preco_pessoa, ID_atracao)
#                 VALUES (%s, %s, %s, %s)
#             """, (id_evento, data_inicio, preco_pessoa_float, id_atracao))

#             conn_db2.commit()
#             print(f"Evento com ID {id_evento} inserido no DB2.")
#         else:
#             print(f"Evento com ID {id_evento} não pode ser inserido porque data_inicio ou preco_pessoa são nulos.")
# # EXCURSÕES
# cursor_db1.execute("""
#     SELECT ID, nome_empresa, capacidade, duracao, site, preco_total, data_inicio, data_termino, ID_atracao
#     FROM excursao
# """)
# excursao_bd1 = cursor_db1.fetchall()

# if not excursao_bd1:
#     print("Nenhuma excursão encontrada em DB1.")
# else:
#     print(f"{len(excursao_bd1)} excursões encontradas em DB1.")

# for excursao in excursao_bd1:
#     id_excursao, nome_empresa, capacidade, duracao, site, preco_total, data_inicio, data_termino, id_atracao = excursao

#     print(f"Processando excursão ID {id_excursao} do DB1.")

#     cursor_db2.execute("""
#         SELECT ID FROM empresa WHERE nome = %s
#     """, (nome_empresa,))
#     empresa_bd2 = cursor_db2.fetchone()

#     if empresa_bd2:
#         id_empresa = empresa_bd2[0]
#         print(f"Empresa {nome_empresa} encontrada no DB2 com ID {id_empresa}.")
#     else:
#         print(f"Empresa {nome_empresa} não encontrada no DB2. Inserindo nova empresa.")
        
#         cursor_db2.execute("""
#             INSERT INTO empresa (nome, site_empresa)
#             VALUES (%s, %s) RETURNING ID
#         """, (nome_empresa, site))
        
#         id_empresa = cursor_db2.fetchone()[0]
#         conn_db2.commit()
#         print(f"Empresa {nome_empresa} inserida no DB2 com ID {id_empresa}.")

#     cursor_db2.execute("""
#         SELECT ID FROM excursao WHERE ID = %s
#     """, (id_excursao,))
#     excursao_bd2 = cursor_db2.fetchone()

#     preco_total_float = float(preco_total.replace('$', '').replace(',', ''))

#     if excursao_bd2:
#         print(f"Excursão com ID {id_excursao} já existe no DB2.")
#         cursor_db2.execute("""
#             UPDATE excursao
#             SET capacidade = %s, qntd_pessoas = %s, preco_total = %s, data_inicio = %s, data_termino = %s, ID_empresa = %s
#             WHERE ID = %s
#         """, (capacidade, duracao, preco_total_float, data_inicio, data_termino, id_empresa, id_excursao))

#         conn_db2.commit()
#         print(f"Excursão com ID {id_excursao} atualizada no DB2.")
#     else:
#         print(f"Excursão com ID {id_excursao} não encontrada no DB2. Inserindo nova excursão.")
#         cursor_db2.execute("""
#             INSERT INTO excursao (ID, capacidade, qntd_pessoas, preco_total, data_inicio, data_termino, ID_atracao, ID_empresa)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#         """, (id_excursao, capacidade, duracao, preco_total_float, data_inicio, data_termino, id_atracao, id_empresa))
        
#         conn_db2.commit()
#         print(f"Excursão com ID {id_excursao} inserida no DB2.")
# # PONTOS TURÍSTICOS
# cursor_db1.execute("""
#     SELECT ID, capacidade, preco_entrada, ID_atracao, data_criacao, data_atualizacao
#     FROM pontos_turisticos
# """)
# pontos_bd1 = cursor_db1.fetchall()

# if not pontos_bd1:
#     print("Nenhum ponto turístico encontrado em DB1.")
# else:
#     print(f"{len(pontos_bd1)} pontos turísticos encontrados em DB1.")

# for ponto in pontos_bd1:
#     id_ponto, capacidade, preco_entrada, id_atracao, data_criacao, data_atualizacao = ponto

#     print(f"Processando ponto turístico ID {id_ponto} do DB1.")

#     cursor_db2.execute("""
#         SELECT ID FROM ponto_turistico WHERE ID_atracao = %s
#     """, (id_atracao,))
#     ponto_bd2 = cursor_db2.fetchone()

#     if ponto_bd2:
#         print(f"Ponto turístico com ID_atracao {id_atracao} já existe no DB2.")
#     else:
#         print(f"Ponto turístico com ID_atracao {id_atracao} não encontrado no DB2. Inserindo novo ponto turístico.")
#         cursor_db2.execute("""
#             INSERT INTO ponto_turistico (ID_atracao)
#             VALUES (%s)
#         """, (id_atracao,))
        
#         conn_db2.commit()
#         print(f"Ponto turístico com ID_atracao {id_atracao} inserido no DB2.")

cursor_db1.close()
cursor_db2.close()
conn_db1.close()
conn_db2.close()
