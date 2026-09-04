# Databricks notebook source


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.silver_drones
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC WITH dados_tratados AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         CAST(id_registro AS BIGINT) AS id_registro,
# MAGIC
# MAGIC         NULLIF(TRIM(id_missao), '') AS id_missao,
# MAGIC
# MAGIC         CAST(timestamp AS TIMESTAMP) AS timestamp,
# MAGIC         CAST(data AS DATE) AS data,
# MAGIC
# MAGIC         CAST(ano AS INT) AS ano,
# MAGIC         CAST(mes AS INT) AS mes,
# MAGIC
# MAGIC         NULLIF(TRIM(id_drone), '') AS id_drone,
# MAGIC         NULLIF(TRIM(tipo_operacao), '') AS tipo_operacao,
# MAGIC         NULLIF(TRIM(zona_operacional), '') AS zona_operacional,
# MAGIC
# MAGIC         CAST(latitude AS DOUBLE) AS latitude,
# MAGIC         CAST(longitude AS DOUBLE) AS longitude,
# MAGIC
# MAGIC         CAST(profundidade_medida_m AS DOUBLE) AS profundidade_medida_m,
# MAGIC         CAST(profundidade_requerida_m AS DOUBLE) AS profundidade_requerida_m,
# MAGIC         CAST(deficit_assoreamento_m AS DOUBLE) AS deficit_assoreamento_m,
# MAGIC
# MAGIC         CAST(intensidade_sinal_sonar_db AS DOUBLE)
# MAGIC             AS intensidade_sinal_sonar_db,
# MAGIC
# MAGIC         CAST(imu_pitch_deg AS DOUBLE) AS imu_pitch_deg,
# MAGIC         CAST(imu_roll_deg AS DOUBLE) AS imu_roll_deg,
# MAGIC         CAST(imu_yaw_deg AS DOUBLE) AS imu_yaw_deg,
# MAGIC
# MAGIC         CAST(velocidade_no AS DOUBLE) AS velocidade_no,
# MAGIC         CAST(rumo_graus AS DOUBLE) AS rumo_graus,
# MAGIC
# MAGIC         NULLIF(TRIM(tipo_fix_gnss), '') AS tipo_fix_gnss,
# MAGIC
# MAGIC         CAST(num_satelites_gnss AS INT) AS num_satelites_gnss,
# MAGIC         CAST(hdop AS DOUBLE) AS hdop,
# MAGIC         CAST(correcao_rtk_cm AS DOUBLE) AS correcao_rtk_cm,
# MAGIC
# MAGIC         CAST(temperatura_agua_c AS DOUBLE) AS temperatura_agua_c,
# MAGIC         CAST(turbidez_ntu AS DOUBLE) AS turbidez_ntu,
# MAGIC         CAST(salinidade_ppt AS DOUBLE) AS salinidade_ppt,
# MAGIC
# MAGIC         CAST(nivel_bateria_pct AS DOUBLE) AS nivel_bateria_pct,
# MAGIC
# MAGIC         NULLIF(TRIM(meio_comunicacao), '') AS meio_comunicacao,
# MAGIC
# MAGIC         CAST(forca_sinal_telemetria_dbm AS DOUBLE)
# MAGIC             AS forca_sinal_telemetria_dbm,
# MAGIC
# MAGIC         CAST(latencia_telemetria_ms AS DOUBLE)
# MAGIC             AS latencia_telemetria_ms,
# MAGIC
# MAGIC         CAST(perda_pacotes_pct AS DOUBLE)
# MAGIC             AS perda_pacotes_pct,
# MAGIC
# MAGIC         CAST(taxa_transmissao_kbps AS DOUBLE)
# MAGIC             AS taxa_transmissao_kbps,
# MAGIC
# MAGIC         NULLIF(TRIM(status_sensor_sonar), '')
# MAGIC             AS status_sensor_sonar,
# MAGIC
# MAGIC         NULLIF(TRIM(status_sensor_imu), '')
# MAGIC             AS status_sensor_imu,
# MAGIC
# MAGIC         NULLIF(TRIM(status_sensor_gnss), '')
# MAGIC             AS status_sensor_gnss,
# MAGIC
# MAGIC         NULLIF(TRIM(status_sensor_rtk), '')
# MAGIC             AS status_sensor_rtk,
# MAGIC
# MAGIC         CAST(indice_qualidade_dados AS DOUBLE)
# MAGIC             AS indice_qualidade_dados,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN LOWER(TRIM(alerta_disparado)) = 'sim' THEN TRUE
# MAGIC             WHEN LOWER(TRIM(alerta_disparado)) = 'nao' THEN FALSE
# MAGIC             ELSE NULL
# MAGIC         END AS alerta_disparado,
# MAGIC
# MAGIC         NULLIF(TRIM(tipo_alerta), '') AS tipo_alerta,
# MAGIC         NULLIF(TRIM(prioridade_alerta), '') AS prioridade_alerta,
# MAGIC
# MAGIC         CAST(score_risco AS DOUBLE) AS score_risco,
# MAGIC
# MAGIC         NULLIF(TRIM(acao_recomendada), '')
# MAGIC             AS acao_recomendada,
# MAGIC
# MAGIC         NULLIF(TRIM(status_dragagem), '')
# MAGIC             AS status_dragagem,
# MAGIC
# MAGIC         CAST(volume_dragado_m3 AS DOUBLE)
# MAGIC             AS volume_dragado_m3,
# MAGIC
# MAGIC         NULLIF(TRIM(operador_responsavel), '')
# MAGIC             AS operador_responsavel,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN LOWER(TRIM(observacoes)) IN ('null', '') THEN NULL
# MAGIC             ELSE TRIM(observacoes)
# MAGIC         END AS observacoes,
# MAGIC
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY id_registro
# MAGIC             ORDER BY timestamp DESC
# MAGIC         ) AS rn
# MAGIC
# MAGIC     FROM default.drones_recife_dataset
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     * EXCEPT (rn),
# MAGIC
# MAGIC     -- DATA COMPLETA PARA ANÁLISE TEMPORAL
# MAGIC     DATE_FORMAT(data, 'yyyy-MM') AS ano_mes,
# MAGIC
# MAGIC     -- CLASSIFICAÇÃO DO SINAL DE TELEMETRIA
# MAGIC     CASE
# MAGIC         WHEN forca_sinal_telemetria_dbm >= -60
# MAGIC             THEN 'Excelente'
# MAGIC         WHEN forca_sinal_telemetria_dbm >= -75
# MAGIC             THEN 'Bom'
# MAGIC         WHEN forca_sinal_telemetria_dbm >= -90
# MAGIC             THEN 'Regular'
# MAGIC         ELSE 'Fraco'
# MAGIC     END AS classificacao_sinal_telemetria,
# MAGIC
# MAGIC     -- CLASSIFICAÇÃO DA BATERIA
# MAGIC     CASE
# MAGIC         WHEN nivel_bateria_pct >= 80 THEN 'Alta'
# MAGIC         WHEN nivel_bateria_pct >= 40 THEN 'Media'
# MAGIC         WHEN nivel_bateria_pct >= 20 THEN 'Baixa'
# MAGIC         ELSE 'Critica'
# MAGIC     END AS classificacao_bateria,
# MAGIC
# MAGIC     -- CLASSIFICAÇÃO DA QUALIDADE DOS DADOS
# MAGIC     CASE
# MAGIC         WHEN indice_qualidade_dados >= 90 THEN 'Excelente'
# MAGIC         WHEN indice_qualidade_dados >= 80 THEN 'Boa'
# MAGIC         WHEN indice_qualidade_dados >= 70 THEN 'Regular'
# MAGIC         ELSE 'Baixa'
# MAGIC     END AS classificacao_qualidade_dados,
# MAGIC
# MAGIC     -- INDICADOR DE ASSOREAMENTO
# MAGIC     CASE
# MAGIC         WHEN deficit_assoreamento_m >= 1 THEN 'Critico'
# MAGIC         WHEN deficit_assoreamento_m >= 0.5 THEN 'Atencao'
# MAGIC         ELSE 'Normal'
# MAGIC     END AS classificacao_assoreamento
# MAGIC
# MAGIC FROM dados_tratados
# MAGIC
# MAGIC WHERE rn = 1
# MAGIC   AND id_registro IS NOT NULL
# MAGIC   AND id_missao IS NOT NULL
# MAGIC   AND id_drone IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.silver_drones
# MAGIC LIMIT 4020;

# COMMAND ----------

# MAGIC %md
# MAGIC INICIO DA CAMADA GOLD
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.gold_resumo_operacional
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC     id_drone,
# MAGIC     zona_operacional,
# MAGIC     tipo_operacao,
# MAGIC
# MAGIC     COUNT(*) AS total_registros,
# MAGIC     COUNT(DISTINCT id_missao) AS total_missoes,
# MAGIC
# MAGIC     ROUND(AVG(profundidade_medida_m), 2)
# MAGIC         AS profundidade_media_m,
# MAGIC
# MAGIC     ROUND(AVG(profundidade_requerida_m), 2)
# MAGIC         AS profundidade_requerida_media_m,
# MAGIC
# MAGIC     ROUND(AVG(deficit_assoreamento_m), 2)
# MAGIC         AS deficit_assoreamento_medio_m,
# MAGIC
# MAGIC     ROUND(MAX(deficit_assoreamento_m), 2)
# MAGIC         AS maior_deficit_assoreamento_m,
# MAGIC
# MAGIC     ROUND(AVG(nivel_bateria_pct), 2)
# MAGIC         AS bateria_media_pct,
# MAGIC
# MAGIC     ROUND(AVG(velocidade_no), 2)
# MAGIC         AS velocidade_media_no,
# MAGIC
# MAGIC     ROUND(AVG(indice_qualidade_dados), 2)
# MAGIC         AS qualidade_media_dados,
# MAGIC
# MAGIC     ROUND(AVG(score_risco), 2)
# MAGIC         AS score_risco_medio,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN alerta_disparado = TRUE THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS total_alertas,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(
# MAGIC             CASE
# MAGIC                 WHEN alerta_disparado = TRUE THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) / COUNT(*),
# MAGIC         2
# MAGIC     ) AS percentual_alertas
# MAGIC
# MAGIC FROM default.silver_drones
# MAGIC
# MAGIC GROUP BY
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC     id_drone,
# MAGIC     zona_operacional,
# MAGIC     tipo_operacao;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.gold_alertas
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC     data,
# MAGIC
# MAGIC     id_drone,
# MAGIC     id_missao,
# MAGIC     zona_operacional,
# MAGIC
# MAGIC     tipo_alerta,
# MAGIC     prioridade_alerta,
# MAGIC
# MAGIC     COUNT(*) AS quantidade_alertas,
# MAGIC
# MAGIC     ROUND(AVG(score_risco), 2)
# MAGIC         AS score_risco_medio,
# MAGIC
# MAGIC     ROUND(MAX(score_risco), 2)
# MAGIC         AS score_risco_maximo,
# MAGIC
# MAGIC     ROUND(AVG(nivel_bateria_pct), 2)
# MAGIC         AS bateria_media_pct,
# MAGIC
# MAGIC     ROUND(AVG(indice_qualidade_dados), 2)
# MAGIC         AS qualidade_media_dados
# MAGIC
# MAGIC FROM default.silver_drones
# MAGIC
# MAGIC WHERE alerta_disparado = TRUE
# MAGIC
# MAGIC GROUP BY
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC     data,
# MAGIC     id_drone,
# MAGIC     id_missao,
# MAGIC     zona_operacional,
# MAGIC     tipo_alerta,
# MAGIC     prioridade_alerta;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.gold_assoreamento
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC
# MAGIC     id_drone,
# MAGIC     id_missao,
# MAGIC     zona_operacional,
# MAGIC
# MAGIC     COUNT(*) AS total_medicoes,
# MAGIC
# MAGIC     ROUND(AVG(profundidade_medida_m), 2)
# MAGIC         AS profundidade_media_m,
# MAGIC
# MAGIC     ROUND(AVG(profundidade_requerida_m), 2)
# MAGIC         AS profundidade_requerida_media_m,
# MAGIC
# MAGIC     ROUND(AVG(deficit_assoreamento_m), 2)
# MAGIC         AS deficit_assoreamento_medio_m,
# MAGIC
# MAGIC     ROUND(MAX(deficit_assoreamento_m), 2)
# MAGIC         AS deficit_assoreamento_maximo_m,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN classificacao_assoreamento = 'Critico'
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS registros_assoreamento_critico,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN tipo_alerta LIKE '%Assoreamento%'
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS alertas_assoreamento,
# MAGIC
# MAGIC     ROUND(SUM(volume_dragado_m3), 2)
# MAGIC         AS volume_total_dragado_m3,
# MAGIC
# MAGIC     status_dragagem
# MAGIC
# MAGIC FROM default.silver_drones
# MAGIC
# MAGIC GROUP BY
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC     id_drone,
# MAGIC     id_missao,
# MAGIC     zona_operacional,
# MAGIC     status_dragagem;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.gold_telemetria
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC
# MAGIC     id_drone,
# MAGIC     zona_operacional,
# MAGIC     meio_comunicacao,
# MAGIC
# MAGIC     COUNT(*) AS total_registros,
# MAGIC
# MAGIC     ROUND(AVG(forca_sinal_telemetria_dbm), 2)
# MAGIC         AS sinal_medio_dbm,
# MAGIC
# MAGIC     ROUND(MIN(forca_sinal_telemetria_dbm), 2)
# MAGIC         AS pior_sinal_dbm,
# MAGIC
# MAGIC     ROUND(AVG(latencia_telemetria_ms), 2)
# MAGIC         AS latencia_media_ms,
# MAGIC
# MAGIC     ROUND(MAX(latencia_telemetria_ms), 2)
# MAGIC         AS latencia_maxima_ms,
# MAGIC
# MAGIC     ROUND(AVG(perda_pacotes_pct), 2)
# MAGIC         AS perda_media_pacotes_pct,
# MAGIC
# MAGIC     ROUND(MAX(perda_pacotes_pct), 2)
# MAGIC         AS perda_maxima_pacotes_pct,
# MAGIC
# MAGIC     ROUND(AVG(taxa_transmissao_kbps), 2)
# MAGIC         AS taxa_media_kbps,
# MAGIC
# MAGIC     ROUND(AVG(nivel_bateria_pct), 2)
# MAGIC         AS bateria_media_pct,
# MAGIC
# MAGIC     classificacao_sinal_telemetria
# MAGIC
# MAGIC FROM default.silver_drones
# MAGIC
# MAGIC GROUP BY
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC     id_drone,
# MAGIC     zona_operacional,
# MAGIC     meio_comunicacao,
# MAGIC     classificacao_sinal_telemetria;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.gold_rastreabilidade
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     id_registro,
# MAGIC     timestamp,
# MAGIC     data,
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     ano_mes,
# MAGIC
# MAGIC     id_missao,
# MAGIC     id_drone,
# MAGIC
# MAGIC     zona_operacional,
# MAGIC
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC
# MAGIC     velocidade_no,
# MAGIC     rumo_graus,
# MAGIC
# MAGIC     profundidade_medida_m,
# MAGIC     profundidade_requerida_m,
# MAGIC     deficit_assoreamento_m,
# MAGIC
# MAGIC     classificacao_assoreamento,
# MAGIC
# MAGIC     tipo_fix_gnss,
# MAGIC     num_satelites_gnss,
# MAGIC     hdop,
# MAGIC
# MAGIC     alerta_disparado,
# MAGIC     tipo_alerta,
# MAGIC     prioridade_alerta,
# MAGIC     score_risco,
# MAGIC
# MAGIC     indice_qualidade_dados
# MAGIC
# MAGIC FROM default.silver_drones;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rastreabilidade
# MAGIC FROM default.gold_rastreabilidade;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_silver
# MAGIC FROM default.silver_drones;

# COMMAND ----------

# MAGIC %md
# MAGIC REGISTRAMOS ACIMA QUE 
# MAGIC silver_drones            = 40.791 registros
# MAGIC gold_rastreabilidade     = 40.791 registros
# MAGIC
# MAGIC ou seja nenhum dado foi perdido na rastreabilidade , abaixo irei validar outros GOLDS
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total
# MAGIC FROM default.gold_resumo_operacional;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total
# MAGIC FROM default.gold_alertas;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total
# MAGIC FROM default.gold_assoreamento;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total
# MAGIC FROM default.gold_telemetria;

# COMMAND ----------

# MAGIC %md
# MAGIC Fazendo o mapeamento de um Calendario , já que a ideia do projeto é acompanhar esses dados ao longo do tempo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.dim_calendario
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     data,
# MAGIC
# MAGIC     YEAR(data) AS ano,
# MAGIC     MONTH(data) AS mes,
# MAGIC
# MAGIC     DATE_FORMAT(data, 'yyyy-MM') AS ano_mes,
# MAGIC
# MAGIC     DATE_FORMAT(data, 'MMMM') AS nome_mes,
# MAGIC
# MAGIC     QUARTER(data) AS trimestre,
# MAGIC
# MAGIC     CONCAT(
# MAGIC         YEAR(data),
# MAGIC         '-T',
# MAGIC         QUARTER(data)
# MAGIC     ) AS ano_trimestre,
# MAGIC
# MAGIC     WEEKOFYEAR(data) AS semana,
# MAGIC
# MAGIC     DAYOFMONTH(data) AS dia,
# MAGIC
# MAGIC     DAYOFWEEK(data) AS dia_semana_num,
# MAGIC
# MAGIC     DATE_FORMAT(data, 'EEEE') AS dia_semana
# MAGIC
# MAGIC FROM (
# MAGIC     SELECT EXPLODE(
# MAGIC         SEQUENCE(
# MAGIC             TO_DATE('2012-01-01'),
# MAGIC             TO_DATE('2026-12-31'),
# MAGIC             INTERVAL 1 DAY
# MAGIC         )
# MAGIC     ) AS data
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_dias,
# MAGIC     MIN(data) AS primeira_data,
# MAGIC     MAX(data) AS ultima_data
# MAGIC FROM default.dim_calendario;

# COMMAND ----------

# MAGIC %md
# MAGIC ATUALIZAÇÕES : foi verificado que quando montei o grafico do mapa locais de recife que não possuem canal, rio nem mar estavam marcados como região detectada pelos drones  ( o que não deveria acontecer ), então estarei corrigindo isso mudando abaixo : 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     id_registro,
# MAGIC     latitude,
# MAGIC     longitude
# MAGIC FROM default.drones_recife_dataset_atualizado
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO default.gold_rastreabilidade AS destino
# MAGIC
# MAGIC USING default.drones_recife_dataset_atualizado AS origem
# MAGIC
# MAGIC ON destino.id_registro = origem.id_registro
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     destino.latitude = origem.latitude,
# MAGIC     destino.longitude = origem.longitude;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_registros,
# MAGIC     COUNT(latitude) AS latitudes,
# MAGIC     COUNT(longitude) AS longitudes
# MAGIC FROM default.gold_rastreabilidade;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     r.id_registro,
# MAGIC     r.latitude,
# MAGIC     r.longitude,
# MAGIC     a.latitude AS latitude_nova,
# MAGIC     a.longitude AS longitude_nova
# MAGIC FROM default.gold_rastreabilidade r
# MAGIC JOIN default.drones_recife_dataset_atualizado a
# MAGIC     ON r.id_registro = a.id_registro
# MAGIC LIMIT 20;