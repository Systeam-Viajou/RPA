import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
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
    print(f"Erro ao conectar ao DB2: {e}")

cursor_db1 = conn_db1.cursor()
cursor_db2 = conn_db2.cursor()

# #-----PLANO-----
# cursor_db1.execute("""
#     SELECT ID, nome, descricao, livre_propaganda, preco, data_criacao, data_atualizacao, duracao
#     FROM plano
# """)
# planos_bd1 = cursor_db1.fetchall()

# if not planos_bd1:
#     print("Nenhum plano encontrado em DB1.")
# else:
#     print(f"{len(planos_bd1)} planos encontrados em DB1 para processamento.")

# for plano in planos_bd1:
#     id_plano, nome, descricao, livre_propaganda, preco, data_criacao, data_atualizacao, duracao = plano

#     cursor_db2.execute("""
#         SELECT nome, descricao, livre_propaganda, valor, duracao, data_desativacao
#         FROM plano WHERE ID = %s
#     """, (id_plano,))
#     plano_bd2 = cursor_db2.fetchone()

#     if plano_bd2:
#         if data_atualizacao is not None and plano_bd2[5] is None:  # Verifica se não está desativado
#             cursor_db2.execute("""
#                 UPDATE plano
#                 SET nome = %s, descricao = %s, livre_propaganda = %s, valor = %s, duracao = %s
#                 WHERE ID = %s
#             """, (nome, descricao, livre_propaganda, preco.replace('$', '').replace(',', ''), duracao, id_plano))

#             conn_db2.commit()
#             print(f"Plano com ID {id_plano} atualizado no DB2.")
#         elif plano_bd2[5] is not None:  # Se estiver desativado, pode ser reativado
#             cursor_db2.execute("""
#                 UPDATE plano
#                 SET nome = %s, descricao = %s, livre_propaganda = %s, valor = %s, duracao = %s, data_desativacao = NULL
#                 WHERE ID = %s
#             """, (nome, descricao, livre_propaganda, preco.replace('$', '').replace(',', ''), duracao, id_plano))
            
#             conn_db2.commit()
#             print(f"Plano com ID {id_plano} reativado no DB2.")
#     else:
#         if data_criacao is not None:
#             preco_formatado = float(preco.replace('$', '').replace(',', ''))

#             cursor_db2.execute("""
#                 INSERT INTO plano (ID, nome, descricao, livre_propaganda, valor, duracao)
#                 VALUES (%s, %s, %s, %s, %s, %s)
#             """, (id_plano, nome, descricao, livre_propaganda, preco_formatado, duracao))

#             conn_db2.commit()
#             print(f"Plano com ID {id_plano} inserido no DB2.")
#         else:
#             print(f"Plano com ID {id_plano} não pode ser inserido porque data_criacao é nula.")

# # Marcação de planos desativados em db2
# cursor_db2.execute("SELECT ID FROM plano")
# ids_db2 = {plano[0] for plano in cursor_db2.fetchall()}
# ids_db1 = {plano[0] for plano in planos_bd1}
# ids_para_desativar = ids_db2 - ids_db1

# for id_del in ids_para_desativar:
#     cursor_db2.execute("""
#         UPDATE plano
#         SET data_desativacao = NOW()
#         WHERE ID = %s AND data_desativacao IS NULL
#     """, (id_del,))
#     conn_db2.commit()
#     print(f"Plano com ID {id_del} marcado como desativado em DB2.")

# -----IMAGEM-----
# Selecionar dados da tabela imagem no PostgreSQL
# cursor_db1.execute("""
#     SELECT url, ID_atracao
#     FROM imagem
# """)
# imagens_bd1 = cursor_db1.fetchall()

# if not imagens_bd1:
#     print("Nenhuma imagem encontrada em DB1.")
# else:
#     print(f"{len(imagens_bd1)} imagens encontradas em DB1 para processamento.")

# # Criar um conjunto de imagens atuais do DB1 (PostgreSQL)
# imagens_atuais_db1 = {(img[0], img[1]) for img in imagens_bd1}  # Conjunto com tuplas (url, ID_atracao)

# # Inserindo/Atualizando as imagens no MongoDB
# for imagem in imagens_bd1:
#     url, id_atracao = imagem

#     # Verificar se a imagem já existe no MongoDB
#     imagem_existente = colecao_imagem.find_one({"url": url, "id_atracao": id_atracao})

#     if not imagem_existente:
#         nova_imagem = {
#             "url": url,
#             "id_atracao": id_atracao
#         }
#         colecao_imagem.insert_one(nova_imagem)
#         print(f"Imagem com URL {url} e ID_atracao {id_atracao} inserida no MongoDB.")
#     else:
#         # Atualiza a imagem apenas se os dados forem diferentes
#         if imagem_existente['url'] != url or imagem_existente['id_atracao'] != id_atracao:
#             colecao_imagem.update_one(
#                 {"url": url, "id_atracao": id_atracao},
#                 {"$set": {"url": url, "id_atracao": id_atracao}}
#             )
#             print(f"Imagem com URL {url} e ID_atracao {id_atracao} atualizada no MongoDB.")

# # Para deletar imagens que foram removidas do DB1 mas ainda existem no MongoDB
# imagens_mongo = colecao_imagem.find({})  # Todas as imagens no MongoDB

# for imagem_mongo in imagens_mongo:
#     url = imagem_mongo['url']
#     id_atracao = imagem_mongo['id_atracao']

#     # Verificar se a imagem está no PostgreSQL (DB1)
#     if (url, id_atracao) not in imagens_atuais_db1:
#         # Se não está, excluir do MongoDB
#         colecao_imagem.delete_one({"url": url, "id_atracao": id_atracao})
#         print(f"Imagem com URL {url} e ID_atracao {id_atracao} deletada do MongoDB.")

# # -----ATRACAO-----
# # Seleciona todas as atrações de DB1
# cursor_db1.execute("""
#     SELECT ID, descricao, nome, endereco, acessibilidade, categoria, data_criacao, data_atualizacao
#     FROM atracao
# """)
# atracoes_bd1 = cursor_db1.fetchall()

# # Verifica se há atrações disponíveis em DB1
# if not atracoes_bd1:
#     print("Nenhuma atração encontrada em DB1.")
# else:
#     print(f"{len(atracoes_bd1)} atrações encontradas em DB1 para processamento.")

# # Processa cada atração encontrada em DB1
# for atracao in atracoes_bd1:
#     id_atracao, descricao, nome, endereco, acessibilidade, categoria, data_criacao, data_atualizacao = atracao

#     # Verifica se a categoria da atração já existe em DB2 e insere se necessário
#     cursor_db2.execute("""
#         SELECT ID FROM categoria WHERE nome = %s
#     """, (categoria,))
#     categoria_bd2 = cursor_db2.fetchone()

#     if not categoria_bd2:
#         cursor_db2.execute("""
#             INSERT INTO categoria (nome) VALUES (%s) RETURNING ID
#         """, (categoria,))
#         categoria_id = cursor_db2.fetchone()[0]
#         conn_db2.commit()
#         print(f"Categoria '{categoria}' inserida no DB2 com ID {categoria_id}.")
#     else:
#         categoria_id = categoria_bd2[0]
#         print(f"Categoria '{categoria}' já existe no DB2 com ID {categoria_id}.")

#     # Verifica se a atração já existe em DB2
#     cursor_db2.execute("""
#         SELECT ID, nome, descricao, endereco, acessibilidade, media_classificacao 
#         FROM atracao WHERE ID = %s
#     """, (id_atracao,))
#     atracao_bd2 = cursor_db2.fetchone()

#     if atracao_bd2 and data_atualizacao is not None:
#         # Atualiza a atração em DB2 se houver mudanças
#         print(f"Atração com ID {id_atracao} encontrada no DB2. Verificando necessidade de atualização.")
#         cursor_db2.execute("""
#             UPDATE atracao
#             SET nome = %s, descricao = %s, endereco = %s, acessibilidade = %s, ID_categoria = %s
#             WHERE ID = %s
#         """, (nome, descricao, endereco, acessibilidade, categoria_id, id_atracao))
#         conn_db2.commit()
#         print(f"Atração com ID {id_atracao} atualizada em DB2.")
#     elif not atracao_bd2:
#         # Insere nova atração em DB2 se não encontrada
#         print(f"Atração com ID {id_atracao} não encontrada em DB2. Preparando para inserção.")
#         if data_criacao is not None:
#             cursor_db2.execute("""
#                 INSERT INTO atracao (ID, nome, descricao, endereco, acessibilidade, ID_categoria)
#                 VALUES (%s, %s, %s, %s, %s, %s)
#             """, (id_atracao, nome, descricao, endereco, acessibilidade, categoria_id))
#             conn_db2.commit()
#             print(f"Atração com ID {id_atracao} inserida com sucesso em DB2.")
#         else:
#             print(f"Atração com ID {id_atracao} não pode ser inserida porque a data de criação é nula.")

# # Obter todos os IDs de atrações de db2
# cursor_db2.execute("SELECT ID FROM atracao")
# ids_db2 = {atracao[0] for atracao in cursor_db2.fetchall()}

# # Utilizar os IDs coletados de db1 durante a inserção/atualização
# ids_db1 = {atracao[0] for atracao in atracoes_bd1}

# # Identificar quais IDs precisam ser deletados de db2
# ids_para_deletar = ids_db2 - ids_db1

# # Deletar as atrações não presentes em db1
# for id_del in ids_para_deletar:
#     cursor_db2.execute("DELETE FROM atracao WHERE ID = %s", (id_del,))
#     conn_db2.commit()
#     print(f"Atração com ID {id_del} removida de DB2.")

# # Verificar categorias que ficaram órfãs após a exclusão das atrações
# cursor_db2.execute("""
#     SELECT c.ID
#     FROM categoria c
#     LEFT JOIN atracao a ON c.ID = a.ID_categoria
#     WHERE a.ID_categoria IS NULL
# """)
# categorias_orfas = {categoria[0] for categoria in cursor_db2.fetchall()}

# # Deletar categorias órfãs
# for cat_id in categorias_orfas:
#     cursor_db2.execute("DELETE FROM categoria WHERE ID = %s", (cat_id,))
#     conn_db2.commit()
#     print(f"Categoria com ID {cat_id} removida de DB2, pois ficou órfã.")

# -----EVENTO-----
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

# #-----EXCURSAO-----
# cursor_db1.execute("""
#     SELECT ID, nome_empresa, capacidade, duracao, site, preco_total, data_inicio, data_termino, ID_atracao
#     FROM excursao
# """)
# excursao_bd1 = cursor_db1.fetchall()

# if not excursao_bd1:
#     print("Nenhuma excursão encontrada em DB1.")
# else:
#     print(f"{len(excursao_bd1)} excursões encontradas em DB1.")

# # Obter IDs das excursões existentes no DB2 para tratar remoção
# cursor_db2.execute("""
#     SELECT ID FROM excursao WHERE data_desativacao IS NULL
# """)
# excursao_ativas_bd2 = [row[0] for row in cursor_db2.fetchall()]

# # Processar excursões do DB1
# for excursao in excursao_bd1:
#     id_excursao, nome_empresa, capacidade, duracao, site, preco_total, data_inicio, data_termino, id_atracao = excursao

#     print(f"Processando excursão ID {id_excursao} do DB1.")

#     # Verificar se a empresa já existe no DB2
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

#     # Verificar se a excursão já existe no DB2
#     cursor_db2.execute("""
#         SELECT ID, data_desativacao FROM excursao WHERE ID = %s
#     """, (id_excursao,))
#     excursao_bd2 = cursor_db2.fetchone()

#     preco_total_float = float(preco_total.replace('$', '').replace(',', ''))

#     if excursao_bd2:
#         # Se excursão já existe e está ativa
#         if excursao_bd2[1] is None:
#             print(f"Atualizando excursão ID {id_excursao} no DB2.")
#             cursor_db2.execute("""
#                 UPDATE excursao
#                 SET capacidade = %s, qntd_pessoas = %s, preco_total = %s, 
#                     data_inicio = %s, data_termino = %s, ID_empresa = %s
#                 WHERE ID = %s
#             """, (capacidade, duracao, preco_total_float, data_inicio, data_termino, id_empresa, id_excursao))
#         else:
#             print(f"Excursão ID {id_excursao} já está desativada no DB2.")
#     else:
#         print(f"Excursão com ID {id_excursao} não encontrada no DB2. Inserindo nova excursão.")
#         cursor_db2.execute("""
#             INSERT INTO excursao (ID, capacidade, qntd_pessoas, preco_total, data_inicio, data_termino, ID_atracao, ID_empresa)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#         """, (id_excursao, capacidade, duracao, preco_total_float, data_inicio, data_termino, id_atracao, id_empresa))
#         conn_db2.commit()
#         print(f"Excursão com ID {id_excursao} inserida no DB2.")

# # Realizar soft delete e criar novo registro com data_desativacao NULL
# excursao_ids_db1 = [excursao[0] for excursao in excursao_bd1]
# for id_excursao_bd2 in excursao_ativas_bd2:
#     if id_excursao_bd2 not in excursao_ids_db1:
#         print(f"Desativando excursão ID {id_excursao_bd2} e criando novo registro.")

#         # Atualizar data_desativacao do registro antigo
#         cursor_db2.execute("""
#             UPDATE excursao 
#             SET data_desativacao = CURRENT_TIMESTAMP
#             WHERE ID = %s
#         """, (id_excursao_bd2,))
        
#         # Buscar dados da excursão original para criar nova entrada
#         cursor_db2.execute("""
#             SELECT capacidade, qntd_pessoas, preco_total, data_inicio, data_termino, ID_atracao, ID_empresa
#             FROM excursao
#             WHERE ID = %s
#         """, (id_excursao_bd2,))
#         excursao_data = cursor_db2.fetchone()

#         # Criar novo registro de excursão sem data_desativacao
#         cursor_db2.execute("""
#             INSERT INTO excursao (capacidade, qntd_pessoas, preco_total, data_inicio, data_termino, ID_atracao, ID_empresa, data_desativacao)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
#         """, (*excursao_data,))
#         conn_db2.commit()
#         print(f"Nova excursão criada e excursão ID {id_excursao_bd2} desativada no DB2.")
# Seleciona pontos turísticos do db1

# # PONTOS TURISTICOS
# cursor_db1.execute("""
#     SELECT ID, ID_atracao, capacidade, preco_entrada, data_criacao, data_atualizacao
#     FROM pontos_turisticos
# """)
# pontos_bd1 = cursor_db1.fetchall()

# if not pontos_bd1:
#     print("Nenhum ponto turístico encontrado em DB1.")
# else:
#     print(f"{len(pontos_bd1)} pontos turísticos encontrados em DB1.")

# for ponto in pontos_bd1:
#     id_ponto, id_atracao, capacidade, preco_entrada, data_criacao, data_atualizacao = ponto

#     print(f"Processando ponto turístico ID {id_ponto} do DB1.")

#     # Verifica se o ponto turístico já existe no db2
#     cursor_db2.execute("""
#         SELECT ID, data_desativacao FROM ponto_turistico WHERE ID = %s
#     """, (id_ponto,))
#     ponto_bd2 = cursor_db2.fetchone()

#     if ponto_bd2:
#         if ponto_bd2[1] is None:  # Ponto está ativo
#             print(f"Ponto turístico ID {id_ponto} encontrado no DB2 e ativo. Atualizando informações.")
#             cursor_db2.execute("""
#                 UPDATE ponto_turistico 
#                 SET data_atualizacao = %s
#                 WHERE ID = %s
#             """, (data_atualizacao, id_ponto))
#             conn_db2.commit()
#             print(f"Ponto turístico ID {id_ponto} atualizado no DB2.")
#         else:  # Ponto está inativo, reativá-lo
#             print(f"Ponto turístico ID {id_ponto} encontrado no DB2, mas inativo. Reativando e atualizando.")
#             cursor_db2.execute("""
#                 UPDATE ponto_turistico 
#                 SET data_atualizacao = %s, data_desativacao = NULL
#                 WHERE ID = %s
#             """, (data_atualizacao, id_ponto))
#             conn_db2.commit()
#             print(f"Ponto turístico ID {id_ponto} reativado e atualizado no DB2.")
#     else:
#         # Insere novo ponto turístico no db2 (ignora campos que não existem em db2)
#         print(f"Ponto turístico ID {id_ponto} não encontrado no DB2. Inserindo como novo ponto turístico.")
#         cursor_db2.execute("""
#             INSERT INTO ponto_turistico (ID, ID_atracao, data_desativacao)
#             VALUES (%s, %s, NULL)
#         """, (id_ponto, id_atracao))
#         conn_db2.commit()
#         print(f"Ponto turístico ID {id_ponto} inserido no DB2.")

cursor_db1.close()
cursor_db2.close()
conn_db1.close()
conn_db2.close()