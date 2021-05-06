
#-----------------------------------------------------------------------
# Carregar bibliotecas twiteer e data 
# #----------------------------------------------------------------------

from twitter import *
from datetime import datetime

#-----------------------------------------------------------------------
# Carregar Credencial 
# #----------------------------------------------------------------------
import sys
sys.path.append(".")
import config
#-----------------------------------------------------------------------
# criar twitter API objeto
#-----------------------------------------------------------------------
twitter = Twitter(auth = OAuth(config.access_key,
                  config.access_secret,
                  config.consumer_key,
                  config.consumer_secret))

#-----------------------------------------------------------------------
# Importar bibliotecas de leitura leitura banco athena
# #-----------------------------------------------------------------------

from pyathena import connect
import pandas as pd
conn = connect(s3_staging_dir='s3://betohotmail/queryresult /', region_name='us-east-2').cursor()


####  a. Tabela1: Consolidado de vendas por ano e mês;

conn.execute("drop table if exists boticario.tb_vendas_cons_mes_ano")
conn.execute("""
        create table  boticario.tb_vendas_cons_mes_ano as 
		SELECT to_char(date_parse(data_venda,'%d/%m/%Y'),'mm-yyyy')  ano_mes, sum(qtd_venda) qtd_vendida  
        FROM "boticario"."tb_vendas" 
		group by data_venda
		order by data_venda 
		""")
print("Info:.. Tabela criada a. Tabela1: Consolidado de vendas por ano e mês; ")
############################################################################################################


####  b. Tabela2: Consolidado de vendas por marca e linha;

conn.execute("drop table if exists boticario.tb_vendas_cons_marca_linha_mes_ano")
conn.execute("""
        create table  boticario.tb_vendas_cons_marca_linha_mes_ano as 
		SELECT id_marca, marca, id_linha, linha , sum(qtd_venda) qtd_vendida  FROM "boticario"."tb_vendas" 
		group by id_marca, id_linha, marca, linha
		order by  id_marca, id_linha 
		""")
print("Info:.. Tabela criada b. Tabela2: Consolidado de vendas por marca e linha; ")
############################################################################################################

####  c. Tabela3: Consolidado de vendas por marca, ano e mês;

conn.execute("drop table if exists boticario.tb_vendas_cons_marca_mes_ano")
conn.execute("""
       create table  boticario.tb_vendas_cons_marca_mes_ano as 
		SELECT MARCA,  to_char(date_parse(data_venda,'%d/%m/%Y'),'mm-yyyy')  ano_mes, sum(qtd_venda) qtd_vendida 
         FROM "boticario"."tb_vendas" 
		group by MARCA,data_venda
	    order by  DATA_VENDA, MARCA
		""")
print("Info:.. Tabela criada c. Tabela3: Consolidado de vendas por marca, ano e mês; ")
############################################################################################################


####  d. Tabela4: Consolidado de vendas por linha, ano e mês; 

conn.execute("drop table if exists boticario.tb_vendas_cons_linha_mes_ano")
conn.execute("""
        create table  boticario.tb_vendas_cons_linha_mes_ano as 
		SELECT LINHA,  to_char(date_parse(data_venda,'%d/%m/%Y'),'mm-yyyy')  ano_mes, sum(qtd_venda) qtd_vendida  
        FROM "boticario"."tb_vendas" 
		group by LINHA,data_venda
		order by  DATA_VENDA, LINHA
		""")
print("Info:.. Tabela criada d. Tabela4: Consolidado de vendas por linha, ano e mês;  ")
############################################################################################################

########RESULTAODO DA BUSCA NO TWITER###############################################################################################

 
#-----------------------------------------------------------------------
# Criterio de busca

conn.execute("select linha from (SELECT linha, max(qtd_vendida) qtd_vendida FROM boticario.tb_vendas_cons_linha_mes_ano where ano_mes ='12-2019' group by linha order by qtd_vendida desc ) limit 1;")
rs = conn.fetchall()
 
for row in rs:
    result= row
 
resultado = 'Boticário '+ result[0]
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
query = twitter.search.tweets(q = resultado,count=50,language='PT')

#-----------------------------------------------------------------------
# Inserir registro na tabela tb_twiter conforme  o resultado da marca mais vendida
#-----------------------------------------------------------------------

#criação  data tabela para inserir resultado da pesquisa
#-----------------------------------------------------------------------
conn.execute("drop table if exists boticario.tb_twitter")
conn.execute("""
            CREATE EXTERNAL TABLE boticario.tb_twitter(
              data string , 
              usuario string COMMENT '', 
              texto string COMMENT '')
              ROW FORMAT DELIMITED 
              FIELDS TERMINATED BY '\;' 
            STORED AS INPUTFORMAT 
              'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
             LOCATION
             's3://betohotmail/twitter'
            """)

conn.execute("msck repair table boticario.tb_twitter")

cont=0
for result in query["statuses"]:
    date_time_str =result["created_at"]
    date_time_obj = datetime. strptime(date_time_str, '%a %b %d %H:%M:%S %z %Y')
    data=datetime.strftime(date_time_obj ,'%Y-%m-%d %H:%M:%S')
    usuario = result["user"]["screen_name"]
    texto= str(result["text"]).strip(' \n\t')
    query ="insert into  boticario.tb_twitter (data,usuario, texto) values ('" + data +  "','" +  usuario  +  "','" +  texto +  "') "
    conn.execute(query)
    cont= cont +1
   
print("Registros gravados referente a busca no twiter é de:" , cont)

