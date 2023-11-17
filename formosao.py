import requests
import pandas as pd
import datetime

import os
from dotenv import load_dotenv

from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

start_time = datetime.datetime.now()

print(f"URL: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
path_to_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Criando uma credencial a partir do arquivo JSON
credentials = service_account.Credentials.from_service_account_file(
    path_to_credentials)

# Inicializando o cliente BigQuery com as credenciais
client = bigquery.Client(credentials=credentials,
                         project=credentials.project_id)

# Defininição das tabelas
project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET_ID')
vendas_table_id = os.getenv("VENDAS_TABLE_ID")
cliente_table_id = os.getenv("CLIENT_TABLE_ID")
produtos_table_id = os.getenv("PRODUTOS_TABLE_ID")
servicos_table_id = os.getenv("SERVICOS_TABLE_ID")
contas_table_id = os.getenv("CONTAS_TABLE_ID")
estoque_table_id = os.getenv("ESTOQUE_TABLE_ID")

client = bigquery.Client(project=project_id)

access_token = os.getenv('ACCESS_TOKEN')
print('token: ' + os.getenv('ACCESS_TOKEN'))


def get_saidas():
    print('Início job get_vendas')
    query = f'SELECT MAX(transaction_date) AS last_transaction_date FROM `{dataset_id}.{vendas_table_id}`'
    result = client.query(query)
    last_transaction_date = result.to_dataframe().iloc[0, 0]

    # Definir a data de início da consulta da API Varejo Online
    ultima_transacao = last_transaction_date + datetime.timedelta(seconds=1)
    desde = ultima_transacao.strftime("%d-%m-%Y %H:%M:%S")
    current_time = datetime.datetime.now()
    ate = current_time.strftime("%d-%m-%Y %H:%M:%S")

    print(f'Data de inicio da consulta: {desde}')

    url_saidas = f'https://integrador.varejonline.com.br/apps/api/saidas?desde={desde}&ate={ate}'
    token = {"token": access_token}
    inicio = 0
    qtd_pag = 300
    data = []

    while True:
        parametros = {
            'inicio': inicio,
            'quantidade': qtd_pag
        }
        response = requests.get(url_saidas, data=token, params=parametros)
        # print(response.text)
        dados = response.json()

        if not dados:
            break
        data.extend(dados)

        if len(dados) < qtd_pag:
            break

        inicio += qtd_pag

    print(f'Quantidade de registros: {len(data)}')

    id_list = []
    date_list = []
    entidade_id_list = []
    terceiro_id_list = []
    status_cancelado_list = []
    tipoSaida_list = []
    cod_venda_list = []
    idProduto_list = []
    idServico_list = []
    valorTotal_list = []
    quantidade_list = []
    valorTotalFrete_list = []
    valorCusto_list = []
    valorTotalDesconto_list = []
    valorTotalAcrescimo_list = []
    valorTotalIcmsSt_list = []
    valorTotalIpi_list = []
    valorTotalSeguro_list = []
    valorTotalOutros_list = []
    codigoSistema_list = []
    descricao_list = []

    for i in data:
        id = i['id']
        date = i['data']
        entidade_id = i['idEntidade']
        terceiro_id = i['idTerceiro']
        cod_venda = i['numeroDocumento']
        status_cancelado = i['cancelado']
        tipoSaida = i['tipoSaida']
        for m in i['mercadorias']:
            idProduto = m['idProduto']
            descricao = m['produto']['descricao']
            codigoSistema = m['produto']['codigoSistema']
            quantidade = m['quantidade']
            valorTotal = m['valorTotal']
            valorTotalIcmsSt = m['valorTotalIcmsSt']
            valorTotalIpi = m['valorTotalIpi']
            valorTotalSeguro = m['valorTotalSeguro']
            valorTotalOutros = m['valorTotalOutros']
            valorTotalAcrescimo = m['valorTotalAcrescimo']
            valorTotalDesconto = m['valorTotalDesconto']
            valorTotalFrete = m['valorTotalFrete']
            valorCusto = m['valorCusto']

            id_list.append(id)
            date_list.append(date)
            entidade_id_list.append(entidade_id)
            terceiro_id_list.append(terceiro_id)
            status_cancelado_list.append(status_cancelado)
            tipoSaida_list.append(tipoSaida)
            cod_venda_list.append(cod_venda)
            idProduto_list.append(idProduto)
            codigoSistema_list.append(codigoSistema)
            descricao_list.append(descricao)
            idServico_list.append(None)
            valorTotal_list.append(valorTotal)
            quantidade_list.append(quantidade)
            valorTotalFrete_list.append(valorTotalFrete)
            valorCusto_list.append(valorCusto)
            valorTotalDesconto_list.append(valorTotalDesconto)
            valorTotalAcrescimo_list.append(valorTotalAcrescimo)
            valorTotalIcmsSt_list.append(valorTotalIcmsSt)
            valorTotalIpi_list.append(valorTotalIpi)
            valorTotalSeguro_list.append(valorTotalSeguro)
            valorTotalOutros_list.append(valorTotalOutros)

        for s in i['servicos']:
            idServico = s['idServico']
            quantidade = s['quantidade']
            valorTotal = s['valorTotal']
            valorTotalAcrescimo = s['valorTotalAcrescimo']
            valorTotalDesconto = s['valorTotalDesconto']
            valorCustoEpoca = s['valorCustoEpoca']

            id_list.append(id)
            date_list.append(date)
            entidade_id_list.append(entidade_id)
            terceiro_id_list.append(terceiro_id)
            status_cancelado_list.append(status_cancelado)
            tipoSaida_list.append(tipoSaida)
            cod_venda_list.append(cod_venda)
            idProduto_list.append(None)
            codigoSistema_list.append(None)
            descricao_list.append(None)
            idServico_list.append(idServico)
            valorTotal_list.append(valorTotal)
            quantidade_list.append(quantidade)
            valorTotalFrete_list.append(None)
            valorCusto_list.append(valorCustoEpoca)
            valorTotalDesconto_list.append(valorTotalDesconto)
            valorTotalAcrescimo_list.append(valorTotalAcrescimo)
            valorTotalIcmsSt_list.append(None)
            valorTotalIpi_list.append(None)
            valorTotalSeguro_list.append(None)
            valorTotalOutros_list.append(None)

    df = pd.DataFrame({
        'transaction_id': id_list,
        'transaction_date': date_list,
        'entity_id': entidade_id_list,
        'client_id': terceiro_id_list,
        'canceled': status_cancelado_list,
        'transaction_type': tipoSaida_list,
        'doc_code': cod_venda_list,
        'product_id': idProduto_list,
        'product_id_internto': codigoSistema_list,
        'product_name': descricao_list,
        'service_id': idServico_list,
        'product_total_value': valorTotal_list,
        'quantity': quantidade_list,
        'st': valorTotalIcmsSt_list,
        'ipi': valorTotalIpi_list,
        'seguro': valorTotalSeguro_list,
        'outros': valorTotalOutros_list,
        'frete_value': valorTotalFrete_list,
        'cost_value': valorCusto_list,
        'discount_amount': valorTotalDesconto_list,
        'addition_value': valorTotalAcrescimo_list
    })

    return df


vendas = get_saidas()
vendas['transaction_date'] = pd.to_datetime(
    vendas['transaction_date'], format='%d-%m-%Y %H:%M:%S')
vendas['quantity'] = vendas['quantity'].astype(float)
vendas['product_total_value'] = vendas['product_total_value'].astype(object)
vendas['st'] = vendas['st'].astype(object)
vendas['ipi'] = vendas['ipi'].astype(object)
vendas['seguro'] = vendas['seguro'].astype(object)
vendas['outros'] = vendas['outros'].astype(object)
vendas['frete_value'] = vendas['frete_value'].astype(object)
vendas['cost_value'] = vendas['cost_value'].astype(object)
vendas['discount_amount'] = vendas['discount_amount'].astype(object)
vendas['addition_value'] = vendas['addition_value'].astype(object)

schema = pd.io.json.build_table_schema(vendas)
table_ref = client.dataset(dataset_id).table(vendas_table_id)
job = client.load_table_from_dataframe(
    vendas, table_ref
)
print(job.result())

# job get_terceiros


def get_terceiros():
    print('Início job get_terceiros')
    query = f'SELECT MAX(data_cadastro) AS last_cadastro_date FROM `{dataset_id}.{cliente_table_id}`'
    result = client.query(query)
    last_cadastro_date = result.to_dataframe().iloc[0, 0]
    ultimo_cadastro = last_cadastro_date + datetime.timedelta(seconds=1)
    alteradoApos = ultimo_cadastro.strftime("%d-%m-%Y %H:%M:%S")

    url_terceiros = f'https://integrador.varejonline.com.br/apps/api/terceiros'
    token = {"token": access_token}
    cont = 0
    inicio = 0
    qtd_pag = 300
    data = []

    while True:
        parametros = {
            'inicio': inicio,
            'quantidade': qtd_pag,
            'alteradoApos': alteradoApos
        }
        response = requests.get(url_terceiros, data=token, params=parametros)

        dados = response.json()

        if not dados:
            break

        data.extend(dados)

        if len(dados) < qtd_pag:
            break
        cont += 1
        print(cont)
        inicio += qtd_pag

    print(f'Quantidade de registros: {len(data)}')

    id_list = []
    nome_list = []
    dataCriacao_list = []
    excluido_list = []
    ativo_list = []
    ddd_list = []
    numero_list = []
    email_list = []
    updated_at = []

    for i in data:
        id = i['id']
        nome = i['nome']
        dataCriacao = i['dataCriacao']
        excluido = i['excluido']
        ativo = i['ativo']
        if i['telefones']:
            ddd = i['telefones'][0]['ddd']
            numero = i['telefones'][0]['numero']
        else:
            ddd = None
            numero = None
        if i['emails']:
            email = i['emails'][0]
        else:
            email = None

        id_list.append(id)
        nome_list.append(nome)
        dataCriacao_list.append(dataCriacao)
        excluido_list.append(excluido)
        ativo_list.append(ativo)
        ddd_list.append(ddd)
        numero_list.append(numero)
        email_list.append(email)
        now = datetime.datetime.now()
        updated_at.append(now)

    df = pd.DataFrame({
        'id': id_list,
        'nome': nome_list,
        'data_cadastro': dataCriacao_list,
        'excluido': excluido_list,
        'ativo': ativo_list,
        'ddd': ddd_list,
        'telefone': numero_list,
        'email': email_list
    })

    return df


terceiros = get_terceiros()
terceiros['data_cadastro'] = pd.to_datetime(
    terceiros['data_cadastro'], format='%d-%m-%Y %H:%M:%S')
table_ref = client.dataset(dataset_id).table(cliente_table_id)
job = client.load_table_from_dataframe(
    terceiros, table_ref
)
print(job.result())


def get_estoque():
    print('Início do job get_estoque')
    url_estoque = 'https://integrador.varejonline.com.br/apps/api/saldos-mercadorias'
    token = {"token": access_token}
    inicio = 0
    qtd_pag = 300
    data = []

    while True:
        parametros = {
            'inicio': inicio,
            'quantidade': qtd_pag
        }
        response = requests.get(url_estoque, data=token, params=parametros)

        dados = response.json()

        if not dados:
            break

        data.extend(dados)

        if len(dados) < qtd_pag:
            break

        inicio += qtd_pag

    print(f'Quantidade de registros: {len(data)}')

    idProduto_list = []
    idEntidade_list = []
    quantidade_list = []
    descricao_list = []
    codigoSistema_list = []
    updated_at = []
    now = datetime.datetime.now()

    for i in data:
        idProduto = i['idProduto']
        idEntidade = i['idEntidade']
        quantidade = i['quantidade']
        descricao = i['produto']['descricao']
        codigoSistema = i['produto']['codigoSistema']

        idProduto_list.append(idProduto)
        idEntidade_list.append(idEntidade)
        quantidade_list.append(quantidade)
        descricao_list.append(descricao)
        codigoSistema_list.append(codigoSistema)
        updated_at.append(now.strftime('%Y-%m-%d %H:%M:%S'))

    df = pd.DataFrame({
        'idProduto': idProduto_list,
        'idEntidade': idEntidade_list,
        'quantidate': quantidade_list,
        'descricao': descricao_list,
        'codigoSistema': codigoSistema_list,
        'updated_at': updated_at
    })

    return df


estoque = get_estoque()
table_ref = client.dataset(dataset_id).table(estoque_table_id)
client.delete_table(table_ref)  # limpar dados antigos
job = client.load_table_from_dataframe(estoque, table_ref)
job.result()

# Criação da Tabela de Produtos
print('Início do job get_produtos')
produtos = estoque.drop(['idEntidade', 'quantidate'], axis=1)
dProdutos = produtos.drop_duplicates()
table_ref = client.dataset(dataset_id).table(produtos_table_id)
client.delete_table(table_ref)  # limpar dados antigos
job = client.load_table_from_dataframe(dProdutos, table_ref)
print(job.result())


def get_servicos():
    print('Início job get_servicos')
    url_servicos = f'https://integrador.varejonline.com.br/apps/api/servicos'
    token = {"token": access_token}
    inicio = 0
    qtd_pag = 300
    data = []

    while True:
        parametros = {
            'inicio': inicio,
            'quantidade': qtd_pag
        }
        response = requests.get(url_servicos, data=token, params=parametros)

        dados = response.json()

        if not dados:
            break

        data.extend(dados)

        if len(dados) < qtd_pag:
            break

        inicio += qtd_pag

    print(f'Quantidade de registros: {len(data)}')

    id_list = []
    descricao_list = []
    updated_at = []

    now = datetime.datetime.now()

    for i in data:
        id = i['id']
        descricao = i['descricao']

        id_list.append(id)
        descricao_list.append(descricao)
        updated_at.append(now.strftime('%Y-%m-%d %H:%M:%S'))

    df = pd.DataFrame({
        'id': id_list,
        'descricao': descricao_list,
        'updated_at': updated_at
    })

    return df


servicos = get_servicos()
table_ref = client.dataset(dataset_id).table(servicos_table_id)
client.delete_table(table_ref)  # limpar dados antigos
job = client.load_table_from_dataframe(
    servicos, table_ref
)
print(job.result())


def get_contas_a_pagar():
    print('Início job get_contas_a_pagar')
    query = f'SELECT MAX(dataAlteracao) AS last_change_date FROM `{dataset_id}.{contas_table_id}`'
    result = client.query(query)
    df = result.to_dataframe()

    if not df.empty and not pd.isna(df['last_change_date'].iloc[0]):
        last_change_date = df['last_change_date'].iloc[0]
        ultima_alteracao = last_change_date + datetime.timedelta(seconds=2)
        startDateQuery = ultima_alteracao.strftime("%d-%m-%Y %H:%M:%S")
        print(f'Data Início da Consulta: {startDateQuery}')
    else:
        startDateQuery = '01-01-1990 00:00:00'
        print(startDateQuery)

    url_contas = 'https://integrador.varejonline.com.br/apps/api/contas-pagar'
    token = {"token": access_token}
    inicio = 0
    qtd_pag = 300
    data = []

    while True:
        params = {
            'inicio': inicio,
            'quantidade': qtd_pag,
            'alteradoApos': startDateQuery
        }

        response = requests.get(url_contas, data=token, params=params)

        dados = response.json()

        if not dados:
            break

        data.extend(dados)

        if len(dados) < qtd_pag:
            break

        inicio += qtd_pag

    print(f'Quantidade de registros: {len(data)}')

    return data


def get_ids_from_json(json):
    data = json
    return [i['id'] for i in data]


def extract_conta_info(data):
    extracted_data = []
    for i in data:
        extracted_data.append({
            'id': i['id'],
            'contaContabilDebito': i['contaContabilDebito'],
            'contaContabilCredito': i['contaContabilCredito'],
            'historico': i['historico'],
            'idEntidade': i['idEntidade'],
            'dataEmissao': i['dataEmissao'],
            'dataBaixa': i['dataBaixa'] if i['baixada'] == True else None,
            'dataAlteracao': i['dataAlteracao'],
            'idControle': i['idControle'],
            'autorizado': i['autorizado'],
            'valor': i['valor'],
            'juros': i['juros'],
            'multa': i['multa'],
            'valorBaixado': i['valorBaixado'] if i['baixada'] == True else None,
            'numeroParcela': i['numeroParcela'],
            'dataVencimento': i['dataVencimento'],
            'idProvisao': i['idProvisao'],
            'tipoDocumento': i['tipoDocumento'],
            'idContaContabilCredito': i['idContaContabilCredito'],
            'valorTotalRetencoes': i['valorTotalRetencoes'],
            'valorTotalParcelas': i['valorTotalParcelas'],
            'diasCarenciaJuros': i['diasCarenciaJuros'],
            'diasCarenciaMulta': i['diasCarenciaMulta'],
            'descontoMaxParcela': i['descontoMaxParcela'],
            'idOperacaoFinanceira': i['idOperacaoFinanceira'],
            'totalParcelas': i['totalParcelas'],
            'baixada': i['baixada']
        })
    return extracted_data


# Criar DataFrame que irá subir para o BD
contas = get_contas_a_pagar()

contas_a_pagar = extract_conta_info(contas)
df_contas = pd.DataFrame(contas_a_pagar)
df_contas['dataEmissao'] = pd.to_datetime(
    df_contas['dataEmissao'], format='%d-%m-%Y')
df_contas['dataBaixa'] = pd.to_datetime(
    df_contas['dataBaixa'], format='%d-%m-%Y')
df_contas['dataAlteracao'] = pd.to_datetime(
    df_contas['dataAlteracao'], format='%d-%m-%Y %H:%M:%S')
df_contas['dataVencimento'] = pd.to_datetime(
    df_contas['dataVencimento'], format='%d-%m-%Y')
df_contas['valorBaixado'] = df_contas['valorBaixado'].astype(object)

# Gerar os IDs da consulta para evitar duplicatas no BD
ids_json = contas
ids_to_delete = get_ids_from_json(ids_json)


def delete_records_by_ids(client, ids_to_delete, schema, table):
    table_ref = client.dataset(schema).table(table)
    table = client.get_table(table_ref)

    delete_query = f"DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE id IN ({', '.join(map(str, ids_to_delete))})"
    query_qtd = f"SELECT COUNT(*) FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE id IN ({', '.join(map(str, ids_to_delete))})"

    qtd_deleted = client.query(query_qtd).result()
    result_qtde = qtd_deleted.to_dataframe().iloc[0, 0]
    print(f'Quantidade de registros excluidos: {result_qtde}')

    client.query(delete_query).result()


def upstream_bigquery(client, df, schema, table):

    table_ref = client.dataset(dataset_id).table(contas_table_id)
    job = client.load_table_from_dataframe(df, table_ref)
    print(job.result())


if ids_to_delete != None:
    delete_records_by_ids(client, ids_to_delete, dataset_id, contas_table_id)

stream_data = upstream_bigquery(client, df_contas, dataset_id, contas_table_id)


print(f'Tempo de execução: {datetime.datetime.now() - start_time}')
