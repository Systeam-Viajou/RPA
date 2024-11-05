import psycopg2
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

load_dotenv()

# =-=-= CONECTAR BANCOS POSTGRESQL E MONGODB=-=-=
def conectar_db1():
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRE_HOST'),
            user=os.getenv('POSTGRE_USER'),
            password=os.getenv('POSTGRE_PASSWORD'),
            port=os.getenv('POSTGRE_PORT'),
            dbname=os.getenv('POSTGRE_DB1')
        )
        print("Conectado ao PostgreSQL DB1 com sucesso!")
        return connection
    except Exception as error:
        print(f"Erro ao conectar ao PostgreSQL DB1: {error}")
        return None

def conectar_db2():
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRE_HOST'),
            user=os.getenv('POSTGRE_USER'),
            password=os.getenv('POSTGRE_PASSWORD'),
            port=os.getenv('POSTGRE_PORT'),
            dbname=os.getenv('POSTGRE_DB2')
        )
        print("Conectado ao PostgreSQL DB2 com sucesso!")
        return connection
    except Exception as error:
        print(f"Erro ao conectar ao PostgreSQL DB2: {error}")
        return None

def conectar_mongo():
    try:
        client = MongoClient(os.getenv('MONGO_HOST'))
        db = client[os.getenv('MONGO_DB')]
        print(f"Conectado ao MongoDB: {os.getenv('MONGO_DB')}")
        return client, db
    except Exception as error:
        print(f"Erro ao conectar ao MongoDB: {error}")
        return None, None

# =-=-=ALTERAR TIPO DE ATRACAO=-=-=
# função para atualizar id_tipo da atracao caso ela seja um evento, tour_virtual, excursao ou ponto turistico
def alterar_tipo_atracao(cursor, id_atracao, id_tipo):
    cursor.execute("""
        UPDATE atracao
        SET ID_tipo = %s
        WHERE ID = %s
    """, (id_tipo, id_atracao))
    
# =-=-=PLANOS=-=-=
def sincronizar_planos(cursor_db1, cursor_db2, conn_db2):
    # Busca todos os planos de DB1
    cursor_db1.execute("""
        SELECT ID, nome, descricao, livre_propaganda, preco, data_criacao, data_atualizacao, duracao
        FROM plano
    """)
    planos_bd1 = cursor_db1.fetchall()

    if not planos_bd1:
        print("Nenhum plano encontrado em DB1.")
        return
    else:
        print(f"{len(planos_bd1)} planos encontrados em DB1 para processamento.")

    # Processa cada plano em DB1
    for plano in planos_bd1:
        id_plano, nome, descricao, livre_propaganda, preco, data_criacao, data_atualizacao, duracao = plano

        # Verifica se o plano já existe no DB2, independente de estar desativado
        cursor_db2.execute("""
            SELECT nome, descricao, livre_propaganda, valor, duracao, data_desativacao
            FROM plano WHERE ID = %s
        """, (id_plano,))
        plano_bd2 = cursor_db2.fetchone()

        if plano_bd2:
            # Se o plano existe e está ativo, atualiza suas informações
            if plano_bd2[5] is None:  # data_desativacao is NULL (plano ativo)
                cursor_db2.execute("""
                    UPDATE plano
                    SET nome = %s, descricao = %s, livre_propaganda = %s, valor = %s, duracao = %s
                    WHERE ID = %s
                """, (nome, descricao, livre_propaganda, preco.replace('$', '').replace(',', ''), duracao, id_plano))
                conn_db2.commit()
                print(f"Plano com ID {id_plano} atualizado no DB2.")
            else:
                # Plano está desativado, cria um novo registro ao invés de reativar
                preco_formatado = float(preco.replace('$', '').replace(',', ''))
                cursor_db2.execute("""
                    INSERT INTO plano (nome, descricao, livre_propaganda, valor, duracao)
                    VALUES (%s, %s, %s, %s, %s)
                """, (nome, descricao, livre_propaganda, preco_formatado, duracao))
                conn_db2.commit()
                print(f"Plano desativado encontrado no DB2. Novo plano criado com base no ID {id_plano}.")
        else:
            # Insere novo plano no DB2 apenas se a data_criacao estiver presente
            if data_criacao is not None:
                preco_formatado = float(preco.replace('$', '').replace(',', ''))
                cursor_db2.execute("""
                    INSERT INTO plano (ID, nome, descricao, livre_propaganda, valor, duracao)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (id_plano, nome, descricao, livre_propaganda, preco_formatado, duracao))
                conn_db2.commit()
                print(f"Novo plano com ID {id_plano} inserido no DB2.")
            else:
                print(f"Plano com ID {id_plano} não pode ser inserido porque data_criacao é nula.")

    # desativar planos db2
    cursor_db2.execute("SELECT ID FROM plano")
    ids_db2 = {plano[0] for plano in cursor_db2.fetchall()}
    ids_db1 = {plano[0] for plano in planos_bd1}
    ids_para_desativar = ids_db2 - ids_db1

    for id_del in ids_para_desativar:
        cursor_db2.execute("""
            UPDATE plano
            SET data_desativacao = NOW()
            WHERE ID = %s AND data_desativacao IS NULL
        """, (id_del,))
        conn_db2.commit()
        print(f"Plano com ID {id_del} marcado como desativado em DB2.")

# =-=-=IMAGENS=-=-=
def sincronizar_imagens(cursor_db1, colecao_imagem):
    # Buscar imagens de DB1
    cursor_db1.execute("SELECT url, ID_atracao FROM imagem")
    imagens_bd1 = cursor_db1.fetchall()

    if not imagens_bd1:
        print("Nenhuma imagem encontrada em DB1.")
        return
    else:
        print(f"{len(imagens_bd1)} imagens encontradas em DB1 para processamento.")

    # Criar conjunto de imagens atuais em DB1
    imagens_atuais_db1 = {(img[0], img[1]) for img in imagens_bd1}

    # Inserir ou atualizar imagens no MongoDB
    for url, id_atracao in imagens_bd1:
        imagem_existente = colecao_imagem.find_one({"url": url, "id_atracao": id_atracao})

        if not imagem_existente:
            colecao_imagem.insert_one({"url": url, "id_atracao": id_atracao})
            print(f"Imagem com URL {url} e ID_atracao {id_atracao} inserida no MongoDB.")
        else:
            # Atualizar a imagem se necessário
            if imagem_existente['url'] != url or imagem_existente['id_atracao'] != id_atracao:
                colecao_imagem.update_one(
                    {"url": url, "id_atracao": id_atracao},
                    {"$set": {"url": url, "id_atracao": id_atracao}}
                )
                print(f"Imagem com URL {url} e ID_atracao {id_atracao} atualizada no MongoDB.")

    # Remover imagens no MongoDB que não estão mais em DB1
    imagens_mongo = colecao_imagem.find({})  # Todas as imagens no MongoDB
    for imagem_mongo in imagens_mongo:
        url = imagem_mongo['url']
        id_atracao = imagem_mongo['id_atracao']

        # Verifica se a imagem está em DB1
        if (url, id_atracao) not in imagens_atuais_db1:
            colecao_imagem.delete_one({"url": url, "id_atracao": id_atracao})
            print(f"Imagem com URL {url} e ID_atracao {id_atracao} deletada do MongoDB.")

# =-=-=ATRACOES=-=-
def sincronizar_atracoes(cursor_db1, cursor_db2, conn_db2):
    cursor_db1.execute("SELECT ID, descricao, nome, endereco, acessibilidade, categoria, data_criacao, data_atualizacao FROM atracao")
    atracoes_bd1 = cursor_db1.fetchall()

    if not atracoes_bd1:
        print("Nenhuma atração encontrada em DB1.")
        return

    for atracao in atracoes_bd1:
        id_atracao, descricao, nome, endereco, acessibilidade, categoria, data_criacao, data_atualizacao = atracao

        # verifica ou insere categoria no DB2
        cursor_db2.execute("SELECT ID FROM categoria WHERE nome = %s", (categoria,))
        categoria_bd2 = cursor_db2.fetchone()
        if not categoria_bd2:
            cursor_db2.execute("INSERT INTO categoria (nome) VALUES (%s) RETURNING ID", (categoria,))
            categoria_id = cursor_db2.fetchone()[0]
            conn_db2.commit()
            print(f"Categoria '{categoria}' inserida no DB2 com ID {categoria_id}.")
        else:
            categoria_id = categoria_bd2[0]

        # verifica se a atração já existe no DB2
        cursor_db2.execute("SELECT ID FROM atracao WHERE ID = %s", (id_atracao,))
        atracao_bd2 = cursor_db2.fetchone()

        if atracao_bd2:
            cursor_db2.execute("""
                UPDATE atracao
                SET nome = %s, descricao = %s, endereco = %s, acessibilidade = %s, ID_categoria = %s
                WHERE ID = %s
            """, (nome, descricao, endereco, acessibilidade, categoria_id, id_atracao))
            conn_db2.commit()
            print(f"Atração com ID {id_atracao} atualizada em DB2.")
        else:
            cursor_db2.execute("""
                INSERT INTO atracao (ID, nome, descricao, endereco, acessibilidade, ID_categoria)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (id_atracao, nome, descricao, endereco, acessibilidade, categoria_id))
            conn_db2.commit()
            print(f"Atração com ID {id_atracao} inserida no DB2.")

    # desativa em DB2
    cursor_db2.execute("SELECT ID FROM atracao")
    ids_db2 = {atracao[0] for atracao in cursor_db2.fetchall()}
    ids_db1 = {atracao[0] for atracao in atracoes_bd1}
    ids_para_desativar = ids_db2 - ids_db1

    for id_del in ids_para_desativar:
        cursor_db2.execute("UPDATE atracao SET data_desativacao = NOW() WHERE ID = %s", (id_del,))
        conn_db2.commit()
        print(f"Atração com ID {id_del} marcada como desativada em DB2.")

# =-=-=EVENTOS=-=-=
def sincronizar_eventos(cursor_db1, cursor_db2, conn_db2):
    # buscar eventos de DB1
    cursor_db1.execute("""
        SELECT ID, data_inicio, horario, data_termino, preco_pessoa, ID_atracao
        FROM eventos
    """)
    eventos_bd1 = cursor_db1.fetchall()

    if not eventos_bd1:
        print("Nenhum evento encontrado em DB1.")
        return
    else:
        print(f"{len(eventos_bd1)} eventos encontrados em DB1 para processamento.")

    # criar conjunto de ids de eventos em DB1 para comparação posterior
    ids_eventos_db1 = {evento[0] for evento in eventos_bd1}

    # processar cada evento de DB1
    for id_evento, data_inicio, horario, data_termino, preco_pessoa, id_atracao in eventos_bd1:
        
        # combinar `data_inicio` e `horario` para criar `TIMESTAMP`
        timestamp_inicio = datetime.combine(data_inicio, horario)

        # converter `preco_pessoa` para float
        preco_pessoa_float = float(preco_pessoa.replace('$', '').replace(',', '').strip()) if preco_pessoa else 0.0

        # verifica se o evento existe no DB2
        cursor_db2.execute("""
            SELECT ID, data_inicio, data_termino, preco_pessoa, ID_atracao, data_desativacao
            FROM evento WHERE ID = %s
        """, (id_evento,))
        evento_bd2 = cursor_db2.fetchone()

        if evento_bd2:
            data_inicio_bd2, data_termino_bd2, preco_pessoa_bd2, id_atracao_bd2, data_desativacao_bd2 = evento_bd2[1:]

            # verifica se há diferenças entre DB1 e DB2
            if (timestamp_inicio != data_inicio_bd2 or
                data_termino != data_termino_bd2 or
                preco_pessoa_float != preco_pessoa_bd2 or
                id_atracao != id_atracao_bd2 or
                data_desativacao_bd2 is not None):
                
                # atualiza evento no DB2
                cursor_db2.execute("""
                    UPDATE evento
                    SET data_inicio = %s, data_termino = %s, preco_pessoa = %s, ID_atracao = %s, data_desativacao = NULL
                    WHERE ID = %s
                """, (timestamp_inicio, data_termino, preco_pessoa_float, id_atracao, id_evento))
                
                conn_db2.commit()
        else:
            # inserir em DB2, se não encontrado
            if data_inicio and preco_pessoa is not None:
                cursor_db2.execute("""
                    INSERT INTO evento (ID, data_inicio, data_termino, preco_pessoa, ID_atracao)
                    VALUES (%s, %s, %s, %s, %s)
                """, (id_evento, timestamp_inicio, data_termino, preco_pessoa_float, id_atracao))
                alterar_tipo_atracao(cursor_db2, id_atracao, 1)
                conn_db2.commit()
                print(f"Evento com ID {id_evento} inserido no DB2")
            else:
                print(f"Evento com ID {id_evento} não pode ser inserido..")

    # soft delete em DB2
    cursor_db2.execute("SELECT ID FROM evento")
    ids_eventos_db2 = {evento[0] for evento in cursor_db2.fetchall()}
    ids_para_desativar = ids_eventos_db2 - ids_eventos_db1

    for id_del in ids_para_desativar:
        cursor_db2.execute("UPDATE evento SET data_desativacao = NOW() WHERE ID = %s", (id_del,))
        conn_db2.commit()
        print(f"Evento com ID {id_del} desativado.")

# =-=-=EXCURSOES=-=-=
def sincronizar_excursao(cursor_db1, cursor_db2, conn_db2):
    # buscar excursões de DB1
    cursor_db1.execute("SELECT ID, nome_empresa, capacidade, duracao, site, preco_total, data_inicio, data_termino, ID_atracao FROM excursao")
    excursao_bd1 = cursor_db1.fetchall()

    if not excursao_bd1:
        print("Nenhuma excursão encontrada em DB1.")
        return
    else:
        print(f"{len(excursao_bd1)} excursões encontradas em DB1 para processamento.")

    # obter IDs das excursões ativas no DB2
    cursor_db2.execute("SELECT ID FROM excursao WHERE data_desativacao IS NULL")
    excursao_ativas_bd2 = {row[0] for row in cursor_db2.fetchall()}

    # processar cada excursão de DB1
    for id_excursao, nome_empresa, capacidade, duracao, site, preco_total, data_inicio, data_termino, id_atracao in excursao_bd1:
        print(f"Processando excursão ID {id_excursao} do DB1.")
        preco_total_float = float(preco_total.replace('$', '').replace(',', ''))

        # verificar se a empresa já existe no DB2
        cursor_db2.execute("SELECT ID FROM empresa WHERE nome = %s", (nome_empresa,))
        empresa_bd2 = cursor_db2.fetchone()

        if empresa_bd2:
            id_empresa = empresa_bd2[0]
        else:
            cursor_db2.execute("INSERT INTO empresa (nome, site_empresa) VALUES (%s, %s) RETURNING ID", (nome_empresa, site))
            id_empresa = cursor_db2.fetchone()[0]
            conn_db2.commit()
            print(f"Empresa '{nome_empresa}' inserida no DB2 com ID {id_empresa}.")

        # verificar se a excursão já existe no DB2
        cursor_db2.execute("SELECT ID, data_desativacao FROM excursao WHERE ID = %s", (id_excursao,))
        excursao_bd2 = cursor_db2.fetchone()

        if excursao_bd2:
            # se excursão existe e está ativa, atualiza
            if excursao_bd2[1] is None:
                cursor_db2.execute("""
                    UPDATE excursao
                    SET capacidade = %s, qntd_pessoas = %s, preco_total = %s, data_inicio = %s, data_termino = %s, ID_empresa = %s
                    WHERE ID = %s
                """, (capacidade, duracao, preco_total_float, data_inicio, data_termino, id_empresa, id_excursao))
                conn_db2.commit()
                print(f"Excursão com ID {id_excursao} atualizada no DB2.")
            else:
                print(f"Excursão ID {id_excursao} já está desativada no DB2.")
        else:
            #se não, insere em DB2
            cursor_db2.execute("""
                INSERT INTO excursao (ID, capacidade, qntd_pessoas, preco_total, data_inicio, data_termino, ID_atracao, ID_empresa)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (id_excursao, capacidade, duracao, preco_total_float, data_inicio, data_termino, id_atracao, id_empresa))
            alterar_tipo_atracao(cursor_db2, id_atracao, 4)
            conn_db2.commit()
            print(f"Excursão com ID {id_excursao} inserida no DB2.")

    # soft delete em DB2
    ids_db1 = {excursao[0] for excursao in excursao_bd1}
    ids_para_desativar = excursao_ativas_bd2 - ids_db1

    for id_del in ids_para_desativar:
        cursor_db2.execute("UPDATE excursao SET data_desativacao = CURRENT_TIMESTAMP WHERE ID = %s", (id_del,))
        conn_db2.commit()
        print(f"Excursão com ID {id_del} marcada como desativada no DB2.")

# =-=-=PONTO TURISTICO=-=-=
def sincronizar_pontos_turisticos(cursor_db1, cursor_db2, conn_db2):
    # Buscar pontos turísticos de DB1
    cursor_db1.execute("""
        SELECT ID, ID_atracao, capacidade, preco_entrada, data_criacao, data_atualizacao
        FROM pontos_turisticos
    """)
    pontos_bd1 = cursor_db1.fetchall()

    if not pontos_bd1:
        print("Nenhum ponto turístico encontrado em DB1.")
        return
    else:
        print(f"{len(pontos_bd1)} pontos turísticos encontrados em DB1 para processamento.")

    # Criar conjunto de IDs de pontos turísticos em DB1 para comparação posterior
    ids_pontos_db1 = {ponto[0] for ponto in pontos_bd1}

    # Processar cada ponto turístico de DB1
    for ponto in pontos_bd1:
        id_ponto, id_atracao, capacidade, preco_entrada, data_criacao, data_atualizacao = ponto
        preco_entrada_float = float(preco_entrada.replace('$', '').replace(',', ''))

        print(f"Processando ponto turístico ID {id_ponto} do DB1.")

        # Verifica se o ponto turístico já existe no DB2 e está ativo
        cursor_db2.execute("""
            SELECT ID, data_desativacao FROM ponto_turistico WHERE ID = %s
        """, (id_ponto,))
        ponto_bd2 = cursor_db2.fetchone()

        # Atualiza o ponto turístico se ele estiver ativo em DB2 e `data_atualizacao` não for nulo em DB1
        if ponto_bd2 and ponto_bd2[1] is None and data_atualizacao is not None:
            cursor_db2.execute("""
                UPDATE ponto_turistico
                SET ID_atracao = %s
                WHERE ID = %s
            """, (id_atracao, id_ponto))
            conn_db2.commit()
            print(f"Ponto turístico ID {id_ponto} atualizado no DB2.")
        
        # Insere novo ponto turístico no DB2 se não encontrado
        elif not ponto_bd2:
            cursor_db2.execute("""
                INSERT INTO ponto_turistico (ID, ID_atracao)
                VALUES (%s, %s)
            """, (id_ponto, id_atracao))
            alterar_tipo_atracao(cursor_db2, id_atracao, 3)
            conn_db2.commit()
            print(f"Ponto turístico ID {id_ponto} inserido no DB2.")

    # Desativação de pontos turísticos que não estão mais em DB1 (independentemente de `data_atualizacao`)
    cursor_db2.execute("SELECT ID FROM ponto_turistico WHERE data_desativacao IS NULL")
    ids_pontos_db2 = {ponto[0] for ponto in cursor_db2.fetchall()}
    ids_para_desativar = ids_pontos_db2 - ids_pontos_db1

    for id_del in ids_para_desativar:
        cursor_db2.execute("UPDATE ponto_turistico SET data_desativacao = NOW() WHERE ID = %s", (id_del,))
        conn_db2.commit()
        print(f"Ponto turístico ID {id_del} marcado como desativado no DB2.")
        
# =-=-=TOUR VIRTUAL=-=-=
def atualizar_tipo_atracao_tour_virtual(cursor_db, mongo_client, conn_db):
    try:
        db = mongo_client[os.getenv('MONGO_DB')]
        collection_tour_virtual = db['tour-virtual']

        # obtém todos os documentos com o campo 'id_atracao' na collection 'tour-virtual'
        atracoes = collection_tour_virtual.find({}, {"id_atracao": 1})

        # verifica se há atrações na collection
        atracoes = list(atracoes) 
        if not atracoes:
            print("Nenhuma atração encontrada na coleção 'tour-virtual'.")
            return
        else:
            print(f"{len(atracoes)} atrações encontradas na coleção 'tour-virtual'.")

        # atualizar o campo ID_tipo na tabela 'atracao' para cada ID_atracao encontrado
        for atracao in atracoes:
            id_atracao = atracao.get("id_atracao")
            if id_atracao:
                alterar_tipo_atracao(cursor_db, id_atracao, 2)
            

        conn_db.commit()

    except Exception as e:
        print(f"Erro ao atualizar ID_tipo para atrações do tour virtual: {e}")


def main():
    conn_db1 = conectar_db1()
    conn_db2 = conectar_db2()
    client_mongo, db_mongo = conectar_mongo()

    try:
        with conn_db1.cursor() as cursor_db1, conn_db2.cursor() as cursor_db2:
            sincronizar_planos(cursor_db1, cursor_db2, conn_db2)
            sincronizar_imagens(cursor_db1, db_mongo['imagem'])
            sincronizar_atracoes(cursor_db1, cursor_db2, conn_db2)
            sincronizar_eventos(cursor_db1, cursor_db2, conn_db2)
            sincronizar_excursao(cursor_db1, cursor_db2, conn_db2)
            sincronizar_pontos_turisticos(cursor_db1, cursor_db2, conn_db2)
            atualizar_tipo_atracao_tour_virtual(cursor_db2, client_mongo, conn_db2)

    finally:
        cursor_db1.close()
        cursor_db2.close()
        conn_db1.close()
        conn_db2.close()
        client_mongo.close()

main()