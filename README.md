# MVP-Nexus

# MVP: Construção de um Pipeline de Dados na Nuvem - Gestão de Chamados de TI

## 1. Contexto de Negócios e Perguntas (Etapa 2 e 4.1)
### 1.1 Contexto do Problema
O setor de Suporte e Operações de TI da organização precisa de monitorar a eficiência no atendimento aos utilizadores, identificar estrangulamentos operacionais e garantir o cumprimento dos Acordos de Nível de Serviço (SLA). Atualmente, a análise é baseada em extrações manuais de planilhas de chamados (fechados, abertos e reabertos), o que dificulta a visão integrada da qualidade do serviço prestado. O objetivo deste MVP é construir um pipeline de dados analítico na nuvem, utilizando a Arquitetura Medalhão, para transformar estes dados operacionais em métricas de desempenho claras.

### 1.2 Perguntas de Negócio
1. Qual é o Tempo Médio de Resolução (MTTR) e a percentagem de chamados que estouram o SLA por Prioridade e Grupo Solucionador?
2. A taxa de reabertura de chamados está dentro da meta aceitável (menor que 5%) para as diferentes equipas de suporte?
3. O cumprimento de SLA geral está de acordo com a meta da empresa (maior ou igual a 85%)?
4. Como se comporta a sazonalidade e o volume de abertura de chamados ao longo dos dias da semana?

### 1.3 Origem, Licença e Aspectos Éticos (LGPD)
Os dados são oriundos de extrações do sistema de ITSM da empresa (operacionalizando o Service Desk). Em estrita conformidade com os preceitos éticos da governança de dados e as diretrizes da LGPD, a base de dados passou por um rigoroso processo de anonimização. Nomes reais, endereços de e-mail e quaisquer informações sensíveis dos colaboradores e solicitantes foram removidos ou substituídos por identificadores sintéticos (ex.: N1, N2-A, N2-RIO).

### 1.4 Resumo dos Dados Brutos
*   **Base_Fechados_2.csv**: Registo de chamados com o ciclo de vida encerrado (id, categoria, prioridade, grupo designado, data de abertura, data de encerramento).
*   **Base_Reabertura_2.csv**: Registo contendo a quantidade de vezes que um mesmo chamado precisou de ser reaberto pelo utilizador.
*   **Base_SLA_2.csv**: Registo com o tempo exato (HH:mm) gasto na resolução do chamado para efeitos de medição de SLA.

---

## 2. Carga dos Dados (Etapa 4.2)
O processo de ingestão foi realizado na plataforma **Databricks Free Edition**. Como primeira etapa, os arquivos CSV foram transferidos para a nuvem utilizando o sistema de **Volumes do Unity Catalog** (caminho: `/Volumes/workspace/default/dados_chamados/`). 
O script completo responsável por esta carga bruta encontra-se no ficheiro de Notebook disponibilizado neste repositório.

![Upload dos Dados](COLOQUE AQUI O LINK DA IMAGEM Base_ArquivosCSV.png)

---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)
### 3.1 Modelagem Dimensional (Star Schema)
Para suportar o consumo analítico (OLAP), foi projetado um Modelo em Estrela (*Star Schema*):
*   **Tabela Fato:** `fato_chamados` (Agrega os eventos operacionais de cada chamado, o tempo de atendimento em horas decimais e as *flags* de reabertura e violação de SLA).
*   **Dimensões:** `dim_grupo` (equipas de suporte) e `dim_tempo` (calendário).

### 3.2 Catálogo de Dados (Metadados DMBOK)
A governança semântica foi implementada via DDL diretamente no *Unity Catalog* do Databricks.

| Tabela | Coluna | Tipo SQL | Domínio / Valores Válidos | Descrição | Linhagem |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `dim_grupo` | `id_grupo` | INT | Inteiro >= 1 | Surrogate Key da equipa | Gerado via ROW_NUMBER() no pipeline |
| `dim_grupo` | `nome_grupo` | STRING | N1, N2-A, N2-RIO | Nome da equipa de atendimento | Extraído e limpo da base Silver |
| `dim_tempo` | `id_tempo` | INT | Formato yyyyMMdd | Surrogate Key temporal | Derivado da `data_abertura` |
| `fato_chamados` | `prioridade` | STRING | 1, 2, 3, 4 | Nível de urgência e impacto | Origem `Base_Fechados` |
| `fato_chamados` | `tempo_atendimento_horas` | DOUBLE | >= 0.00 | Duração do atendimento em horas | Convertido da string HH:mm na Silver |
| `fato_chamados` | `reaberto_flag` | INT | 0 (Não) ou 1 (Sim) | Indicador se o chamado foi reaberto | Junção com a `Base_Reabertura` |
| `fato_chamados` | `sla_horas_limite` | DECIMAL | 1.0, 2.0, 4.0, 8.0 | Teto máximo permitido por prioridade | Regra de negócio inserida na Gold |
| `fato_chamados` | `sla_cumprido_flag` | INT | 0 (Estourou) ou 1 (No prazo) | Indicador binário de sucesso de SLA | Cálculo lógico na tabela fato |

![Catálogo de Dados no Unity Catalog](COLOQUE AQUI O LINK DA IMAGEM Tabela_Fato_Overview.png)

---

## 4. Pipeline de Dados (Etapa 4.4)
O fluxo de ETL foi estruturado em cadernos (*notebooks*) PySpark e SQL, respeitando a **Arquitetura Medalhão**:

*   **Camada Bronze (`helpdesk_bronze.raw_*`):** Leitura iterativa dos CSVs através do Unity Catalog Volumes. Os espaços nos nomes das colunas originais foram substituídos por _ (exigência do formato Delta) e foram injetados metadados essenciais de rastreabilidade (`_data_ingestao`).
*   **Camada Silver (`helpdesk_silver.chamados_consolidados`):** Responsável por sanear a qualidade. Aqui ocorreu a desduplicação (`dropDuplicates`), o *join* (cruzamento) dos três ficheiros através do ID do chamado, a tipagem estrita de colunas string para *Date* e a conversão complexa do texto de tempo ("HH:mm") para um valor duplo numérico representando horas decimais.
*   **Camada Gold (`helpdesk_gold.*`):** A tabela consolidada Silver foi modelada num *Star Schema*. As regras de negócio estritas foram aplicadas com `CASE WHEN` para estabelecer dinamicamente o SLA permitido por prioridade (1h, 2h, 4h e 8h) e validar o seu cumprimento.

*Referência dos Scripts:* Todo o código executável (`.py` / `.sql`) encontra-se no notebook principal no raiz deste repositório.

---

## 5. Qualidade de Dados (Etapa 4.5)
A qualidade dos dados foi validada e tratada de acordo com as dimensões preconizadas pelo DMBOK:
*   **Unicidade:** Os ficheiros brutos continham riscos de duplicação do ID do chamado. Foi aplicado sistematicamente o comando `.dropDuplicates(["id_chamado"])` na entrada da camada Silver.
*   **Consistência e Formatação:** O sistema de origem gerava nomes de colunas com espaços e parênteses. Foi criada uma função PySpark iterativa de *cleansing* de *headers* na subida para a Bronze para garantir compatibilidade estrutural com o formato Delta Lake.
*   **Acurácia e Domínio:** O campo de tempo gasto na `Base_SLA` apresentava-se no formato texto `HH:mm`. Para que as métricas tivessem rigor matemático (acurácia), utilizou-se a função `F.split` para separar horas e minutos e dividir os minutos por `60.0`, obtendo um valor numérico exato em decimais (ex.: "02:30" tornou-se `2.5` horas).
*   **Completude:** Casos em que o chamado não teve registo de reabertura resultavam em valores Nulos (*NULL*) após o cruzamento (`LEFT JOIN`). Aplicou-se o `F.coalesce()` para forçar a substituição de *NULL* por `0`, mantendo a integridade do cálculo das médias e taxas.

---

## 6. Análise de Dados (Etapa 4.5)
Com a camada Gold materializada, o pipeline analítico demonstrou valor de negócio ao responder de forma declarativa (SQL) aos indicadores de qualidade:

1. **Reabertura (Meta < 5%):** Como se observa nos resultados, todas as equipas e níveis de prioridade conseguiram manter o indicador "DENTRO DA META" (abaixo dos 5%), com excepção de um pico pontual de 5.19% da equipa N2-A na Prioridade 3.
2. **Cumprimento de SLA (Meta >= 85%):** A análise evidenciou gargalos operacionais graves. Apesar de as Prioridades 3 e 4 representarem o grande volume de atendimento da operação, a equipa N1 (Nível 1) apresentou o estado "FORA DA META", registando percentagens de cumprimento perigosamente baixas na Prioridade 1 (76%) e Prioridade 3 (51%).
3. **Tempo Médio de Resolução:** O indicador de tempo médio revela que os incidentes de Prioridade 3 demoram, por vezes, cerca de 95 horas no N1, um valor muitíssimo superior à meta estabelecida de 4 horas (provocando o chumbo na meta de SLA).
4. A análise de sazonalidade revelou que a Segunda-Feira concentra o maior volume de aberturas de chamados na semana, com 1631 tickets. Este dado é vital para a gestão, pois permite readequar a escala técnica de primeira linha (N1) para evitar a degradação da taxa de cumprimento do SLA neste dia específico.

![Consulta Analítica com Metas de SLA](COLOQUE AQUI O LINK DA IMAGEM Celula_5_Final.png)

![Dashboard Analítico de SLA por Prioridade e Grupo](COLOQUE AQUI O LINK DA IMAGEM Celula_5_SLA.png)

![Dashboard Analítico da Sazonalidade](COLOQUE AQUI O LINK DA IMAGEM Consulta_Extra.png)

---

## 7. Autoavaliação
O presente MVP permitiu alcançar na íntegra o objetivo de conceber um pipeline de dados na nuvem para a equipe de Qualidade e Suporte de TI. A aplicação da Arquitetura Medalhão mostrou-se essencial para separar o ruído dos ficheiros Excel de origem das tabelas curadas utilizadas na tomada de decisão. 

As maiores dificuldades encontradas decorreram das idiossincrasias do próprio *Databricks Free Edition* (como as restrições na configuração do formato *Delta Lake*), que exigiram a escrita de funções *custom* em PySpark para higienização dos nomes de cabeçalho na camada Bronze, e na tipagem de dados temporais complexos ("HH:mm"). 

O resultado final traduz-se numa forte melhoria face ao controlo anterior. No entanto, durante o desenvolvimento deste MVP, identifiquei uma oportunidade de evolução analítica direta para a minha prática profissional: **a análise cruzada da equipa vs. tempo de resolução**. Verificou-se que o incumprimento do SLA (especialmente no nível N1) não deve ser analisado isoladamente. Como trabalho futuro imediato, pretendo cruzar o *Tempo Médio de Resolução* e o *Cumprimento de SLA* com o **volume de chamados (*headcount*) por analista técnico** presente na `dim_tecnico`. Esta análise permitirá responder de forma cabal à gestão se as falhas de SLA decorrem de ineficiência de processo ou de subdimensionamento crónico das equipas (sobrecarga de trabalho por analista).
