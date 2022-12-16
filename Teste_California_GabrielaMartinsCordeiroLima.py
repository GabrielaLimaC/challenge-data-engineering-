import boto3
import pandas as pd
import awswrangler as wr

url = 'https://raw.githubusercontent.com/MaisTodos/challenge-data-engineering/main/california_housing_train.csv'

#lendo arquivo
arq = pd.read_csv(url)
df = pd.DataFrame(arq)

#Qual a coluna com maior desvio padrão?
df.std(axis = 0, skipna = True)

#Qual valor mínimo e o máximo?
df.min()
df.max()

# Criando a coluna hma_cat, baseada na coluna housing_median_age, conforme as regras abaixo: 
#  Se < 18 então de_0_ate_18.
#  Se >= 18 E < 29 entao ate_29.
#  Se >= 29 E < 37 entao ate_37.
#  Se >= 37 então acima_37.   

df['hma_cat'] = None

for i in range(0,len(df['housing_median_age'])): 
    if df['housing_median_age'][i] < 18:
        df['hma_cat'][i] = 'de_0_ate_18'
    elif df['housing_median_age'][i] >= 18 and df['housing_median_age'][i] < 29:
        df['hma_cat'][i] = 'ate_29'
    elif df['housing_median_age'][i] >= 29 and df['housing_median_age'][i] < 37:
        df['hma_cat'][i] = 'ate_37'
    elif df['housing_median_age'][i] >= 37:
        df['hma_cat'][i] = 'acima_37'

#Criando a coluna c_ns: Onde longitude abaixo (<) de -119 recebe o valor norte e acima(>=) sul. 
df['c_ns'] = None

for i in range(0,len(df['longitude'])): 
    if df['longitude'][i] < -119:
        df['c_ns'][i] = 'norte'
    else:
        df['c_ns'][i] = 'sul'
        
#renomeando colunas
df.rename(columns={
        'hma_cat': 'age',
        'c_ns': 'california_region'}, inplace = True)

#ajustando o tipo das colunas
df['age'] = df['age'].astype('string')
df['california_region'] = df['california_region'].astype('string')
df['total_rooms'] = df['total_rooms'].astype('float64')
df['total_bedrooms'] = df['total_bedrooms'].astype('float64')
df['population'] = df['population'].astype('float64')
df['households'] = df['households'].astype('float64')
df['median_house_value'] = df['median_house_value'].astype('float64')

#Escreva um arquivo no formato Parquet localmente considerando o dataframe final, crie a seguinte analise:
# Age
# California_region
# S_population: Soma de population
# M_median_house_value: Média de median_house_value

df_final = df[['age','california_region','population','median_house_value']]
df_final = df_final.groupby(['age','california_region']).agg({'population':'sum', 'median_house_value': 'mean'}).sort_values(by='median_house_value',ascending=False)
df_final.rename(columns={
        'population': 's_population',
        'median_house_value': 'm_median_house_value'}, inplace = True)

df_final['s_population'] = df_final['s_population'].astype('float')
df_final['m_median_house_value'] = df_final['m_median_house_value'].astype('float')

df_fim = pd.DataFrame(df_final)
