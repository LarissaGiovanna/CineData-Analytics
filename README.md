# Pipeline de Dados CineData Analytics (Databricks + PySpark)
Pipeline ETL em Arquitetura Medalhão (Bronze, Silver e Gold) para estruturar, no Data Lakehouse do Databricks, um catálogo de filmes (base combinada TMDB/IMDb).

Este projeto foi desenvolvido para o desafio de Engenharia de Dados do **Rocket Lab 2026.2 (Visagio)**. Os dados brutos chegam de forma intencionalmente suja e fragmentada em 5 arquivos CSV. O objetivo é ingeri-los, limpá-los e deixá-los prontos para análises de BI e para alimentar um assistente de IA.


## Como funciona
1. Os 5 arquivos CSV são carregados em um Volume do Databricks (zona *Landing*).
2. O notebook `Landing_to_Bronze` lê cada CSV sem alterar o conteúdo, adiciona a coluna `ingestion_datetime` e grava em tabelas Delta com o modo `append`.
3. O mesmo notebook busca a cotação do dólar na API do Banco Central e grava em `bronze.tb_cotacao_dolar`.
4. O notebook `Bronze_to_Silver` lê `bronze.tb_movies_info`, limpa, padroniza e tipa os dados e grava em `silver.tb_info_filmes`.
5. Funções de verificação de qualidade conferem o resultado e imprimem `[PASS]` ou `[FAIL]` para cada regra.

## Status da entrega

| Etapa | Item | Status |
|---|---|---|
| Landing → Bronze | 5 tabelas dos CSVs (`tb_movies_info`, `tb_movies_financials`, `tb_movies_metrics`, `tb_credits_and_tags`, `tb_movies_reviews`) | Concluído |
| Landing → Bronze | `tb_cotacao_dolar` (API do Banco Central, com alternativa por arquivo) | Concluído |
| Bronze → Silver | `tb_info_filmes` | Concluído |
| Bronze → Silver | `tb_metricas_engajamento` | Não concluído |
| Bronze → Silver | `tb_financeiro_filmes`, `tb_avaliacoes_usuarios`, `tb_generos`, `tb_pessoas_empresas`, `tb_cotacao_dolar` | Não concluído |
| Silver → Gold | Star Schema e `gold_genai_movies_context` | Não concluído |
| Orquestração | Job com as tarefas `to_Bronze`, `to_Silver`, `to_Gold` e agendamento | Não concluído |
| Analytics | Perguntas de negócio | Não concluído |

## Como executar
### Requisitos:
1. Um workspace Databricks com Unity Catalog (o ambiente usado foi a **Databricks Free Edition**, com compute serverless)
2. Permissão para criar schemas e Volumes
3. Os 5 arquivos CSV da pasta `Inputs` do desafio

### 1. Preparar o ambiente
1. No Databricks, escolha (ou crie) um catalog. O notebook `Landing_to_Bronze` cria o schema `bronze` caso ele ainda não exista.
2. Crie um Volume (por exemplo, `Landing`) dentro do schema `bronze`.
3. Vá em **Catalog** → seu schema → seu Volume → **Upload to this volume** e envie os 5 CSVs.
4. Importe os notebooks `Landing_to_Bronze.ipynb` e `Bronze_to_Silver.ipynb` para o seu workspace.
5. Nos dois notebooks, confira as variáveis do topo (catalog, schemas e caminho do Volume) e ajuste para o seu ambiente.

### 2. Camada Bronze (`Landing_to_Bronze`)
A Bronze é uma **cópia fiel** do dado bruto: nenhuma limpeza é feita aqui. Cada CSV vira uma tabela Delta:

| Arquivo de origem | Tabela Bronze |
|---|---|
| `movies_info_IMDB_TMDB.csv` | `bronze.tb_movies_info` |
| `movies_financials_IMDB_TMDB.csv` | `bronze.tb_movies_financials` |
| `movies_metrics_IMDB_TMDB.csv` | `bronze.tb_movies_metrics` |
| `credits_and_tags_IMDB_TMDB.csv` | `bronze.tb_credits_and_tags` |
| `movies_reviews.csv` | `bronze.tb_movies_reviews` |
| API do Banco Central | `bronze.tb_cotacao_dolar` |

O que o notebook faz:
- **Leitura sem `inferSchema`:** todas as colunas chegam como texto. Assim, valores sujos (como "Unknown" em colunas numéricas) não são perdidos nem convertidos em silêncio. A tipagem é responsabilidade da Silver.
- **Opção `escape` na leitura:** as sinopses e os comentários têm aspas dentro do texto, e sem essa opção o Spark quebrava registros e misturava colunas. As tabelas foram recriadas depois dessa correção.
- **Coluna `ingestion_datetime`:** recebe o timestamp do momento da gravação. Ela é adicionada na mesma cadeia que termina no `.write()`, porque, pela avaliação preguiçosa (*lazy evaluation*) do Spark, o valor só é calculado quando a gravação dispara.
- **Gravação em Delta com `append`:** cada execução adiciona novas linhas, sem apagar as anteriores.

*Nota:* como a Bronze usa `append`, executar o notebook novamente **duplica as linhas**. Isso é esperado: a deduplicação é feita na Silver.

Para conferir se as tabelas foram criadas, rode em uma célula SQL:
```sql
SELECT COUNT(*) FROM <seu_catalog>.bronze.tb_movies_info;  -- troque pelo seu catalog
```


### 3. Cotação do dólar (API do Banco Central)
A diretoria financeira quer acompanhar orçamento e receita também em Reais, então o notebook consulta a cotação do dólar (PTAX) e grava em `bronze.tb_cotacao_dolar`.

1. Dois **widgets** recebem a data de início e a data de fim, no formato `MM-DD-AAAA`. Por padrão, o período é de **7 dias corridos** até a data de execução, já que a API não retorna cotação em finais de semana e feriados.
2. As datas formatadas entram na URL da API:
```text
https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='{data_inicio_formatada}'&@dataFinalCotacao='{data_fim_formatada}'&$select=dataHoraCotacao,cotacaoCompra&$format=json
```
3. A resposta em JSON traz os registros na chave `value`, cada um com `dataHoraCotacao` e `cotacaoCompra`. Eles viram um DataFrame com schema definido, recebem `ingestion_datetime` e são gravados em Delta com `append`.

**OBS.:** na Databricks Free Edition, o compute serverless **não tem acesso à internet externa**, e a chamada à API falha com erro de resolução de nome (`Failed to resolve 'olinda.bcb.gov.br'`). Por isso, o notebook usa `try/except`: ele tenta a API e, se houver erro de conexão, lê um arquivo JSON de contingência com a resposta da mesma API, obtido em `[data em que o JSON foi obtido]`. Em um ambiente sem essa restrição, a chamada à API funciona normalmente e o arquivo não é usado.

### 4. Camada Silver (`Bronze_to_Silver`): `silver.tb_info_filmes`
A Silver é a versão **limpa e confiável** do dado: colunas em português, tipos corretos, valores inválidos tratados e sem duplicatas. Esta tabela vem de `bronze.tb_movies_info`:

| Coluna de origem | Coluna de destino | Tipo |
|---|---|---|
| `id` | `id_filme` | string |
| `title` | `titulo` | string |
| `original_title` | `titulo_original` | string |
| `release_date` | `data_lancamento` | date |
| `runtime` | `duracao_minutos` | int |
| `original_language` | `idioma_original` | string |
| `status` | `status_filme` | string |
| `overview` | `sinopse` | string |
| `tagline` | `frase_divulgacao` | string |
| (derivada de `data_lancamento`) | `ano_lancamento` | int |

Regras aplicadas:
1. **Chave:** `trim` no `id_filme` e descarte de linhas com id nulo, porque um filme sem chave não pode ser usado nas tabelas seguintes.
2. **Deduplicação:** uma janela (`Window`) particionada por `id_filme` e ordenada pela data de ingestão, da mais recente para a mais antiga, com `row_number()`. Só a linha número 1 é mantida. Não foi usado `dropDuplicates`, porque ele não garante qual linha permanece, e o desafio pede a versão mais recente. Validação: o total de linhas final é igual ao número de ids distintos.
3. **Status:** primeiro a **normalização** (minúsculas, hífens e underscores viram espaço, caracteres não alfabéticos são removidos, espaços repetidos são reduzidos e há `trim`), depois a **tradução**:

   | Normalizado | Tradução |
   |---|---|
   | released | Lançado |
   | post production | Pós-Produção |
   | in production | Em Produção |
   | planned | Planejado |
   | rumored | Rumores |
   | canceled | Cancelado |
   | qualquer outro valor (inclusive nulo) | Não Informado |

4. **Data de lançamento:** a origem tem vários formatos (por exemplo, `yyyy-MM-dd` e `MM-dd-yyyy`). Cada formato é tentado com `try_to_timestamp`, e o `coalesce` fica com o primeiro que funcionar. Só o que nenhum formato reconhece vira `NULL`. Depois, `ano_lancamento` é extraído com `year()`.
5. **Duração:** conversão segura para inteiro com `try_cast`. Valores que não são números viram `NULL`.
6. **Gravação:** `overwrite` com `overwriteSchema` em `silver.tb_info_filmes`.


### 5. Verificações de qualidade
Depois de gravada, a tabela é lida de volta e passa por verificações adaptadas do material da aula (`dq_check` e `dq_check_unique`). Cada uma conta as linhas que **violam** a regra e imprime `[PASS]` ou `[FAIL]`:

| Verificação | Regra |
|---|---|
| `id_filme` não nulo | nenhum id nulo |
| `id_filme` único | nenhuma chave repetida |
| `status_filme` em domínio válido | apenas os 6 status traduzidos e "Não Informado" |
| `duracao_minutos >= 0` | nenhuma duração negativa |
| `ano_lancamento` plausível | entre 1880 e 2100 |

Resultado: **todas as verificações deram PASS**.

**OBS.:** nas verificações de faixa, valores `NULL` não contam como falha, porque `NULL` é o resultado esperado da limpeza de dados inválidos. Para exigir preenchimento, usa-se uma verificação explícita de "não nulo", como a do `id_filme`.


## Decisões e limitações
- **Silver reconstruída a cada execução:** como a Bronze acumula linhas com `append`, a Silver usa `overwrite` e é refeita por inteiro a partir dela. Rodar o notebook duas vezes dá o mesmo resultado, sem duplicar.
- **Conversões seguras:** `try_cast` e `try_to_timestamp` devolvem `NULL` em vez de erro, para que um valor sujo não interrompa o pipeline.
- **Linhas com colunas deslocadas (*column shift*):** a origem tem linhas com valores fora de lugar (por exemplo, uma data no campo de status). Foram encontrados 2 casos em `tb_info_filmes`. Eles ficam com `NULL` ou "Não Informado" nos campos que não podem ser convertidos. Realinhar os valores foi considerado e descartado: não é pedido no desafio, é arriscado (não dá para saber onde cada deslocamento começa) e campos excedentes podem já ter sido perdidos na leitura do CSV. **Limitação:** se um valor deslocado parecer válido (por exemplo, um número na coluna de duração), ele não é detectável.
- **Datas ambíguas:** para textos como `03-04-2020`, a **ordem dos formatos** na lista é a regra. Foi assumido mês-dia, porque é o padrão da origem (por exemplo, `12-25-2017`).
- **Duração igual a 0:** `0` minutos foi mantido como `0`, porque o desafio não pede outro tratamento para esta tabela.
- **Coluna de data de ingestão:** `[descartada no select final / mantida como coluna de auditoria — ajuste conforme o seu notebook]`.
- **API bloqueada na Free Edition:** por isso existe o arquivo de contingência, descrito na seção da cotação do dólar.

## Próximos passos
O que ficou pendente e como estava planejado:
- **`silver.tb_metricas_engajamento`** (origem: `tb_movies_metrics`):
  - conversão segura de tipos (`double` para notas e popularidade, `int` para contagens de votos);
  - notas do TMDB e do IMDb fora do intervalo de 0 a 10 (inclusive as multiplicadas por erro de escala) viram `NULL`;
  - contagens de votos e popularidade negativas viram `NULL`;
  - popularidade limpa por **padrões de formato** (vírgula decimal, separador de milhar), sem remover caracteres de forma genérica, para que textos deslocados não virem números plausíveis.
- **Demais tabelas da Silver:** `tb_financeiro_filmes`, `tb_avaliacoes_usuarios`, `tb_generos`, `tb_pessoas_empresas` e `tb_cotacao_dolar` (com preenchimento *forward fill* para finais de semana e feriados).
- **Camada Gold:** Star Schema (fato, dimensões e tabelas-ponte), tabela `gold_genai_movies_context` e as perguntas de negócio.
- **Orquestração:** Job no Databricks Workflows com as tarefas `to_Bronze`, `to_Silver` e `to_Gold`, dependências explícitas e agendamento.

## Estrutura do repositório
```text
├── notebooks/
│   ├── Landing_to_Bronze.ipynb
│   └── Bronze_to_Silver.ipynb
├── data/
│   └── cotacao_dolar.json      # arquivo de contingência da API
└── README.md
```