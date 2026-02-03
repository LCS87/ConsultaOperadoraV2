# ConsultaOperadoraV2

## 📋 Sobre o Projeto

**ConsultaOperadoraV2** é uma aplicação Java desenvolvida para processar arquivos CSV contendo dados de CNPJs e classificar empresas por operadora de telefonia. O sistema é especializado no processamento de dados da região Nordeste do Brasil, identificando automaticamente a operadora de telefonia (CLARO, VIVO, TIM, OI, FIXO ou SEM OPERADORA) associada a cada CNPJ.

O projeto utiliza processamento paralelo para otimizar o desempenho e suporta múltiplos formatos de arquivo CSV, com detecção automática de delimitadores e validação de dados.

## ✨ Funcionalidades

### Principais Características

- 🔄 **Processamento em Lote**: Processa múltiplos arquivos CSV simultaneamente
- 🔍 **Detecção Automática de Delimitadores**: Identifica automaticamente vírgula, ponto e vírgula ou tabulação
- 📱 **Classificação por Operadora**: Identifica a operadora de telefonia com base nos números de telefone
- 🗑️ **Deduplicação**: Remove CNPJs duplicados durante o processamento
- ⚡ **Processamento Paralelo**: Utiliza thread pool para maior performance
- 📊 **Log Detalhado**: Gera log completo de processamento com métricas e estatísticas
- 📞 **Suporte a Múltiplos Formatos**: Aceita diferentes formatos de telefone:
  - `(DD) NNNN-NNNN`
  - `DD-NNNNNNNN`
- 🎯 **Filtro Regional**: Processa apenas arquivos da região Nordeste (AL, BA, CE, MA, PB, PE, PI, RN, SE)
- ✅ **Validação de CNPJ**: Valida e filtra apenas CNPJs com 14 dígitos

### Operadoras Suportadas

- **CLARO**
- **VIVO**
- **TIM**
- **OI**
- **FIXO**
- **SEM OPERADORA**

## 🛠️ Tecnologias Utilizadas

- **Java**: Linguagem de programação principal
- **OpenCSV**: Biblioteca para leitura e escrita de arquivos CSV
- **libphonenumber** (Google Phone Number Library): Biblioteca para validação e identificação de operadoras de telefone
- **Maven**: Gerenciamento de dependências e build

## 📦 Requisitos

- **Java**: JDK 8 ou superior
- **Maven**: 3.6 ou superior (para build e gerenciamento de dependências)
- **Sistema Operacional**: Windows, Linux ou macOS

## 🚀 Instalação

### Pré-requisitos

Certifique-se de ter o Java e o Maven instalados:

```bash
java -version
mvn -version
```

### Clonando o Repositório

```bash
git clone <url-do-repositorio>
cd ConsultaOperadoraV2
```

### Compilando o Projeto

```bash
mvn clean compile
```

### Gerando o JAR Executável

```bash
mvn clean package
```

O arquivo JAR será gerado em `target/ConsultaOperadoraV2-<versao>.jar`

## ⚙️ Configuração

Antes de executar a aplicação, é necessário configurar os caminhos de entrada e saída no arquivo `Main.java`:

```java
private static final String INPUT_FOLDER = "E:\\Projetos JAVA\\CnpjDownloader\\cnpj_data\\export";
private static final String OUTPUT_FOLDER = "E:\\Projetos JAVA\\CnpjDownloader\\arquivos_finalizados";
```

**Importante**: Ajuste esses caminhos conforme sua estrutura de diretórios.

### Configurações Adicionais

- **THREAD_POOL_SIZE**: Número de threads para processamento paralelo (padrão: 6)
- **Formato de Arquivos de Entrada**: Os arquivos CSV devem seguir o padrão de nomenclatura:
  - `{UF}_*.csv` (ex: `al_ativos.csv`)
  - `*_{UF}.csv` (ex: `ativos_al.csv`)

## 📖 Como Usar

### Estrutura de Entrada

Os arquivos CSV de entrada devem conter as seguintes colunas (mínimo 6 colunas):

1. `cnpj_completo` - CNPJ completo (14 dígitos)
2. `razao_social` - Razão social da empresa
3. `endereco_completo` - Endereço completo
4. `email` - E-mail da empresa
5. `ano_abertura` - Ano de abertura
6. `telefones` - Números de telefone (pode conter múltiplos números)

### Executando a Aplicação

```bash
java -jar target/ConsultaOperadoraV2-<versao>.jar
```

Ou, se estiver usando Maven:

```bash
mvn exec:java -Dexec.mainClass="Main"
```

### Saída

A aplicação gera arquivos CSV separados por operadora no diretório de saída configurado:

- `{UF} - CLARO.csv`
- `{UF} - VIVO.csv`
- `{UF} - TIM.csv`
- `{UF} - OI.csv`
- `{UF} - FIXO.csv`
- `{UF} - SEM_OPERADORA.csv`

Cada arquivo contém apenas os CNPJs classificados para aquela operadora específica.

### Log de Processamento

Um arquivo de log detalhado é gerado em `{OUTPUT_FOLDER}/processamento.log` contendo:

- Timestamp de cada operação
- Progresso do processamento
- Estatísticas por arquivo
- Relatório final com métricas consolidadas
- Tratamento de erros

## 📁 Estrutura do Projeto

```
ConsultaOperadoraV2/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── Main.java          # Classe principal
│   │   └── resources/
│   └── test/
│       ├── java/
│       └── resources/
├── target/                         # Arquivos compilados
├── pom.xml                         # Configuração Maven
├── .gitignore
├── LICENSE
└── README.md
```

## 🔧 Funcionalidades Técnicas

### Detecção de Delimitadores

O sistema detecta automaticamente o delimitador usado no arquivo CSV testando:
- Vírgula (`,`)
- Ponto e vírgula (`;`)
- Tabulação (`\t`)

### Validação de Telefones

- Normalização automática de formatos diversos
- Aplicação da regra do nono dígito para números móveis
- Validação usando a biblioteca libphonenumber
- Fallback para identificação baseada em prefixos quando necessário

### Processamento Paralelo

Utiliza `ExecutorService` com pool de threads configurável para processar múltiplos arquivos simultaneamente, otimizando o tempo de execução.

### Deduplicação

Mantém um `HashSet` de CNPJs processados para garantir que cada CNPJ apareça apenas uma vez nos arquivos de saída.

## 📊 Métricas e Relatórios

Ao final do processamento, o sistema exibe:

- Tempo total de processamento
- Número de arquivos processados com sucesso
- Total de registros processados
- Quantidade de arquivos CSV gerados
- Taxa de sucesso do processamento
- Distribuição de registros por operadora

## ⚠️ Observações Importantes

1. **Encoding**: Os arquivos CSV são lidos com encoding `ISO-8859-1`
2. **Delimitador de Saída**: Os arquivos gerados usam ponto e vírgula (`;`) como delimitador
3. **Região**: Apenas arquivos da região Nordeste são processados
4. **Validação**: CNPJs inválidos ou duplicados são automaticamente descartados
5. **Performance**: Para grandes volumes de dados, ajuste o `THREAD_POOL_SIZE` conforme o hardware disponível

## 🐛 Tratamento de Erros

O sistema possui tratamento robusto de erros:

- Arquivos vazios ou corrompidos são logados e pulados
- Erros de parsing são capturados e registrados
- Falhas em arquivos individuais não interrompem o processamento dos demais
- Stack traces completos são registrados no log para depuração

## 📝 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 👥 Contribuindo

Contribuições são bem-vindas! Sinta-se à vontade para:

1. Fazer fork do projeto
2. Criar uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abrir um Pull Request

## 📧 Contato

Para dúvidas, sugestões ou problemas, abra uma [issue](../../issues) no repositório.

---

**Desenvolvido para processamento eficiente de dados de CNPJs**
