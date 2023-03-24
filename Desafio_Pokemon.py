# Databricks notebook source
spark.conf.set("fs.azure.account.key.databoxhml.blob.core.windows.net","8woRC1uvbZahU9tE/LIzosTmZyfGapMQFBRk8n8sXGLRlm6tUFFq2eHAVOadbAP2YEYRv1a9Nwld+AStdxm2Ww==")
df = spark.read.format('delta').load('wasbs://databox@databoxhml.blob.core.windows.net/pokemons').cache()
df.display()

# COMMAND ----------

import pyspark.sql.functions as fn

# COMMAND ----------

df.show(10, False)

# COMMAND ----------

df.select('id' , fn.col('altura').alias('AT')).display()

# COMMAND ----------

# DBTITLE 1,Filtros
df.filter("id <= 3").display()
#Ou então df.filter(fn.col('id') == 3).display()

df.filter("id <= 3").select('id' , 'nome').display()

# COMMAND ----------

# DBTITLE 1,Agregação
df.groupBy('nome').agg(fn.sum('peso').alias('qtd')).display()

# alteração Ju 1,Agregação
df.groupBy('nome').agg(fn.sum('altura').alias('qtd')).display()

# COMMAND ----------

# DBTITLE 1,Ordenação
df.orderBy(fn.col('peso').desc()).display()

# COMMAND ----------

# DBTITLE 1,Agregação + Ordenação
(df
 .groupBy('nome')
 .agg(fn.sum('peso').alias('Peso'),
      fn.count('id').alias('Qtd'))
 .orderBy(fn.col('Qtd').desc())
 .select('Peso')
).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 1
# quantos pokemons existem no total?
df.agg(fn.count('id').alias('qtd_pokemons')).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 2
# quantos kgs pesam todos os pokemons juntos?
df.agg(fn.sum('peso').alias('peso_total')).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 3
# retorne os pokemons que não possuem experiência
df.filter(" experiencia is null").display()

# COMMAND ----------

# DBTITLE 1,Pergunta 4
# retorne o(s) pokemon(s) mais pesado(s)

max_peso = df.agg(fn.max('peso')).collect()
# ou df.agg(fn.max('peso')).first()[0]
max_peso[0][0]

(df
 .groupBy('nome')
 .agg(fn.sum('peso').alias('Peso'))
 .orderBy(fn.col('Peso').desc())
 .filter(fn.col('Peso') == max_peso[0][0])
 .select('nome','Peso')
).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 5
# retorne o(s) pokemon(s) mais alto(s)
max_alt = df.agg(fn.max('altura')).collect()[0][0]
max_alt[0][0]

(df
 .groupBy('nome')
 .agg(fn.sum('altura').alias('Altura'))
 .orderBy(fn.col('altura').desc())
 .filter(fn.col('altura') == df.agg(fn.max('altura')).collect()[0][0])
 .select('nome','altura')
).display()

# COMMAND ----------

# DBTITLE 1,Pergunta 6
# quantos pokemons tem mais de 200 pontos de experiencia? E quais são?
df_ex6 = (df
 .filter("experiencia > 200")
 .select('nome','experiencia')
  )
df_ex6.display()

df_ex6.count()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Pergunta 7
# quantos pokemons possuem mais de 20m e menos de 1000kg? E quais são?
df_ex_7 = (df
 .filter("altura > 20 and peso < 1000")
 .select('nome','altura' ,'peso')
  )

df_ex_7 = (df
 .filter("altura > 20")
 .filter("peso < 1000")        
 .select('nome','altura' ,'peso')
  )

df_ex_7.display()

df_ex_7.distinct().count()

# COMMAND ----------

# DBTITLE 1,Pergunta 8
# retorne os pokemons que possuem mais de uma forma
df_size = df.withColumn('tamanho_lista_forma' , fn.size('formas'))

df_size.filter(fn.col('tamanho_lista_forma') > 1).display()
