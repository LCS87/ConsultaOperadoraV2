# CNPJ Processor - Classificador por Operadora de Telefonia

Processador de arquivos CSV contendo dados de CNPJs que classifica empresas por operadora de telefonia (CLARO, VIVO, TIM, OI, FIXO, SEM OPERADORA) para a região Nordeste do Brasil.

## 📋 Funcionalidades

- **Processamento em lote**: Processa múltiplos arquivos CSV simultaneamente
- **Detecção automática de delimitadores**: Identifica automaticamente vírgula, ponto e vírgula ou tabulação
- **Classificação por operadora**: Identifica a operadora de telefonia com base nos números
- **Deduplicação**: Remove CNPJs duplicados durante o processamento
- **Processamento paralelo**: Usa thread pool para maior performance
- **Log detalhado**: Gera log completo de processamento
- **Suporte a diferentes formatos de telefone**: (DD) NNNN-NNNN ou DD-NNNNNNNN

## 🗂️ Estrutura do Projeto
