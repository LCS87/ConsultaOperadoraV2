# 📋 Plano de Melhoria — ConsultaOperadoraV2

**Data:** 04/11/2026
**Escopo:** Evolução do processador CLI de CSVs (CNPJ × Operadora) para uma plataforma **Backend Spring Boot + Frontend Angular**, com API REST documentada via OpenAPI/Swagger.

---

## 1. Diagnóstico Atual (o que existe hoje)

| Item | Situação | Risco / Problema |
|---|---|---|
| `src/main/java/Main.java` | Arquivo único com **517 linhas**, todas as classes estáticas, sem pacotes | Baixa manutenibilidade, difícil testar e evoluir |
| `pom.xml` | ⚠️ **Contém o conteúdo do `.gitignore`, não é um POM válido** | O projeto **não compila com Maven** (`mvn clean package` falha) |
| Configuração | Caminhos hardcoded em Windows (`E:\Projetos JAVA\...`) | Não portável; impossível rodar em servidor/Docker |
| Logs | `PrintWriter` próprio + `showProgress()` manual | Sem níveis, sem estrutura, thread-unsafe |
| Paralelismo | `ExecutorService` fixo (6 threads), estado global em `AtomicLong/AtomicInteger` | Métricas acopladas à lógica; difícil monitorar |
| Testes | **Zero** testes automatizados | Regressões silenciosas em regras de negócio (CNPJ, nono dígito, operadora) |
| Interface | Nenhuma (CLI puro) | Usuário final depende de programador para operar |
| Persistência | Arquivos CSV soltos no disco | Sem histórico, sem consulta, sem deduplicação persistente entre execuções |

---

## 2. Objetivo do Plano

Transformar o sistema em uma aplicação **client–server**:

- **Backend**: API REST Spring Boot que executa o pipeline de classificação (CSV → CNPJ válido → operadora), persiste resultados e expõe métricas/histórico.
- **Frontend**: Painel Angular para upload de arquivos, acompanhamento do processamento em tempo real, consulta/filtro de registros por operadora/UF e download dos resultados.
- **Qualidade**: código modular em camadas, coberto por testes, com documentação OpenAPI/Swagger automática.

### Stack alvo (conforme tabela aprovada)

| Camada | Tecnologia | Finalidade |
|---|---|---|
| Backend | Java 17 (LTS) | Linguagem principal |
| Backend | Spring Boot 3.x | Construção de APIs e aplicações |
| Backend | Spring Framework | Injeção de dependências e infraestrutura |
| Backend | Spring Security | Autenticação e autorização |
| Backend | Spring Data JPA | Persistência de dados |
| Backend | Hibernate | ORM |
| Frontend | Angular 17+ | Framework frontend |
| Frontend | TypeScript | Linguagem do frontend |
| Frontend | RxJS | Programação reativa e assincronicidade |
| Frontend | Angular Material | Componentes visuais |
| Comunicação | REST API | Comunicação frontend/backend |
| Comunicação | OpenAPI / Swagger (springdoc) | Documentação de APIs |

> As bibliotecas atuais (**OpenCSV** e **libphonenumber**) são mantidas — elas contêm a regra de negócio central e serão encapsuladas em serviços dedicados.

---

## 3. Estratégia: Fases Incrementais

Princípio: **nunca quebrar o que já funciona**. Cada fase termina com algo utilizável e testado.

```
FASE 0 ──► FASE 1 ──► FASE 2 ──► FASE 3 ──► FASE 4 ──► FASE 5 ──► FASE 6 ──► FASE 7
 Fundac.   Refator.   Testes    Spring      Persist.  Segurança  Frontend    Docs/Obs.
 (urgente) (core)    (unit.)   Boot/API    (JPA)     (Security) (Angular)   (Swagger)
```

---

### 🟥 FASE 0 — Fundação (urgente, ~1 dia)

**Meta:** fazer o projeto compilar e ser configurável novamente.

1. Recriar o `pom.xml` válido (Java 17, OpenCSV, libphonenumber, JUnit 5).
2. Extrair configuração para `application.properties` / variáveis de ambiente:
   - `app.input-folder`, `app.output-folder`, `app.thread-pool-size`, `app.ufs-permitidas`.
3. Substituir log manual por **SLF4J + Logback** (níveis, arquivo rolante).
4. Adicionar `.editorconfig` e convenção de pacotes: `br.com.consultaoperadora.*`.

**Critério de aceite:** `mvn clean package` gera JAR executável; caminhos vêm de config, não de código.

---

### 🟧 FASE 1 — Refatoração do Núcleo (~3 dias)

**Meta:** dividir o `Main.java` monolítico em classes coesas, com responsabilidade única.

| Nova classe | Responsabilidade (extraída do Main.java) |
|---|---|
| `model/EmpresaRegistro` | DTO do registro de CNPJ |
| `model/Operadora` | Enum CLARO, VIVO, TIM, OI, FIXO, SEM_OPERADORA |
| `service/CnpjValidator` | Validação de 14 dígitos + dígitos verificadores |
| `service/TelefoneNormalizer` | Normalização, regra do nono dígito |
| `service/OperadoraClassifier` | libphonenumber + fallback por prefixo |
| `service/CsvDelimiterDetector` | Detecção de `,`, `;`, `\t` |
| `service/CsvReaderService` / `CsvWriterService` | Leitura/escrita OpenCSV (ISO-8859-1 entrada, `;` saída) |
| `service/DeduplicationService` | Deduplicação (hoje HashSet global) |
| `batch/ProcessamentoBatchService` | Orquestração + ExecutorService |
| `batch/MetricasProcessamento` | Contadores atômicos isolados |
| `Main` → `CommandLineRunner` | Apenas bootstrap |

**Regra:** comportamento **idêntico** ao atual (mesmos arquivos de saída `{UF} - {OPERADORA}.csv`). Validado por teste de regressão com dataset de exemplo na Fase 2.

---

### 🟨 FASE 2 — Testes Automatizados (~2 dias)

**Meta:** proteger as regras de negócio antes de mexer na arquitetura web.

- Unitários: `CnpjValidator` (casos válidos/inválidos/dígitos), `TelefoneNormalizer` (todos os formatos suportados + nono dígito), `OperadoraClassifier` (por operadora e UF), `CsvDelimiterDetector`.
- Integração: pipeline completo sobre CSV de exemplo (`src/test/resources/exemplo_ne/`), comparando saída esperada.
- Ferramentas: JUnit 5, AssertJ, Mockito. Meta mínima: **80% de cobertura nas classes de serviço**.

**Critério de aceite:** `mvn verify` verde e relatório JaCoCo publicado em `target/site/jacoco`.

---

### 🟩 FASE 3 — Backend Spring Boot + API REST (~4 dias)

**Meta:** expor o motor como serviço HTTP.

Estrutura do módulo `backend/`:

```
backend/
└── src/main/java/br/com/consultaoperadora/
    ├── config/          (OpenApiConfig, AsyncConfig, SecurityConfig)
    ├── controller/      (UploadController, ProcessamentoController, RegistroController, DashboardController)
    ├── service/         (camadas da Fase 1 promovidas a @Service)
    ├── repository/      (Spring Data JPA — Fase 4)
    ├── model/ entity/ dto/
    └── exception/       (GlobalExceptionHandler @RestControllerAdvice)
```

Endpoints principais:

| Método | Rota | Descrição |
|---|---|---|
| POST | `/api/v1/arquivos/upload` | Upload de um ou vários CSVs (multipart) |
| POST | `/api/v1/processamentos` | Dispara job de processamento (assíncrono, retorna `jobId`) |
| GET | `/api/v1/processamentos/{id}` | Status/métricas do job |
| GET | `/api/v1/processamentos/{id}/sse` | Progresso em tempo real (Server-Sent Events) |
| GET | `/api/v1/registros?operadora=&uf=&page=` | Busca paginada/filterável (Pageable) |
| GET | `/api/v1/registros/export?operadora=&uf=` | Download CSV do filtro |
| GET | `/api/v1/dashboard/resumo` | Contagens por operadora/UF para gráficos |

Padrões: respostas `ResponseEntity<DTO>`, erros RFC-7807 (ProblemDetail), versionamento de URL `/api/v1`, validação `jakarta.validation`.

**Critério de aceite:** ciclo completo upload → processar → consultar via `curl`/Swagger UI funcionando localmente.

---

### 🟩 FASE 4 — Persistência (Spring Data JPA + Hibernate) (~3 dias)

**Meta:** substituir saída apenas-arquivo por banco consultável (PostgreSQL em dev/prod, H2 em testes).

Entidades:
- `ArquivoOrigem` (nome, UF, encoding, delimitador detectado, status)
- `ProcessamentoJob` (status NOVO/EM_EXECUCAO/CONCLUIDO/FALHA, métricas, timestamps)
- `EmpresaRegistro` (cnpj 14 dígitos UNIQUE, razão social, endereço, e-mail, ano abertura, telefones, `operadora` @Enumerated, uf, FK job)
- `DuplicidadeLog` (auditoria de descartes — substitui o HashSet volátil por deduplicação **persistente** entre execuções)

Itens técnicos: Flyway para migrações (`V1__init.sql`), índices em `(operadora, uf)` e `cnpj`, paginação Spring Data, batch insert (`hibernate.jdbc.batch_size=50`), exportação CSV lida do banco.

**Critério de aceite:** reprocessar o mesmo arquivo não duplica CNPJs (dedupe vem do banco, não da memória).

---

### 🟦 FASE 5 — Spring Security (~2 dias)

**Meta:** proteger a API.

- Autenticação **JWT** (access + refresh token), endpoints `/api/v1/auth/**` liberados.
- Perfis: `ROLE_ADMIN` (upload, processar, excluir) e `ROLE_USER` (consultar, exportar).
- Autorização por método (`@PreAuthorize`), CORS restrito ao frontend, headers de segurança, rate limiting básico no upload.
- Usuários iniciais via seed do Flyway com `BCryptPasswordEncoder`.

**Critério de aceite:** chamadas sem token retornam 401; usuário comum não consegue disparar processamento (403).

---

### 🟦 FASE 6 — Frontend Angular (~5 dias)

**Meta:** painel operacional completo.

Estrutura do módulo `frontend/`:

```
frontend/src/app/
├── core/        (interceptors JWT+erro, guards, services: AuthService, ApiClient)
├── shared/      (pipes CNPJ/telefone, componentes reutilizáveis)
├── features/
│   ├── login/
│   ├── upload/        (drag-and-drop multi-arquivo, barra de progresso RxJS)
│   ├── processamento/ (mat-table + SSE/EventSource p/ progresso em tempo real)
│   ├── registros/     (filtro por operadora/UF, paginação servidor, export)
│   └── dashboard/     (gráficos de distribuição por operadora)
└── app.routes.ts      (lazy loading + canActivate authGuard)
```

Decisões técnicas:
- **TypeScript strict**, tipagem gerada a partir do contrato OpenAPI.
- **RxJS**: `HttpClient` observables, `switchMap`/`catchError` em pipelines, `EventSource` wrapped em Observable para progresso; `async pipe` nos templates; `takeUntilDestroyed` contra vazamentos.
- **Angular Material**: `mat-table` (com `MatPaginator`/`MatSort` server-side), `mat-dialog`, `mat-snack-bar`, `stepper` do wizard de upload, tema claro/escuro.
- Estado: Services + `BehaviorSignal`/Signals (sem NgRx nesta versão; fica como melhoria futura opcional).

**Critério de aceite:** operador não-técnico consegue subir CSVs, acompanhar o job e baixar resultados sem tocar em linha de comando.

---

### 🟪 FASE 7 — Documentação, Observabilidade e Entrega (~2 dias)

- **springdoc-openapi** → Swagger UI em `/swagger-ui.html`; schema versionado e compartilhado com o time de front.
- **Actuator** (`/actuator/health`, `/metrics`) + métricas Micrometer do batch (tempo de processamento, registros/s).
- **Docker Compose**: `postgres` + `backend` + `frontend (nginx)` para subir tudo com um comando.
- CI simples (GitHub Actions): build + testes + análise estática a cada PR.
- Atualização do README com arquitetura (diagrama), passos de execução e contrato de API resumido.

---

## 4. Estrutura Final do Repositório

```
ConsultaOperadoraV2/
├── PLANO_DE_MELHORIA.md        ← este documento
├── backend/                    ← projeto Spring Boot (Maven)
│   ├── pom.xml                 ← POM corrigido e completo
│   └── src/main/java/br/com/consultaoperadora/...
├── frontend/                   ← projeto Angular (npm)
│   └── src/app/...
├── legacy/                     ← Main.java original preservado até a Fase 3 validar paridade
├── docker-compose.yml
└── .github/workflows/ci.yml
```

---

## 5. Cronograma Resumido

| Fase | Entregável | Estimativa | Dependência |
|---|---|---|---|
| 0 | Build saudável + config externa | 1 dia | — |
| 1 | Núcleo refatorado em camadas | 3 dias | F0 |
| 2 | Suíte de testes ≥80% nos serviços | 2 dias | F1 |
| 3 | API REST funcional (motor via HTTP) | 4 dias | F2 |
| 4 | Persistência JPA/Hibernate + Flyway | 3 dias | F3 |
| 5 | Auth JWT + autorização | 2 dias | F3 |
| 6 | Painel Angular completo | 5 dias | F3–F5 |
| 7 | Swagger, Docker, CI, docs | 2 dias | Todas |
| **Total** | | **~20 dias úteis** | |

---

## 6. Riscos e Mitigações

| Risco | Impacto | Mitigação |
|---|---|---|
| Mudança de comportamento na refatoração (regras de operadora/CNPJ) | Alto | Testes de regressão com dataset real **antes** de mover código (Fase 2 primeiro em parte) |
| Volume grande de CSVs sobrecarrega request HTTP | Médio | Processamento 100% assíncrono (job + SSE); upload em chunk se necessário |
| libphonenumber nem sempre retorna operadora (portabilidade numérica) | Médio | Manter fallback por prefixo atual + marcar `SEM_OPERADORA` explicitamente |
| Encoding ISO-8859-1 vs UTF-8 no banco | Médio | Normalizar para UTF-8 na ingestão; teste dedicado |
| Escopo crescer (auth, Docker, CI…) | Médio | Fases independentes com aceite claro; cada fase entrega valor isolado |

---

## 7. Métricas de Sucesso

- ✅ `mvn clean verify` verde com cobertura ≥80% dos serviços de regra de negócio
- ✅ Tempo de processamento equivalente ou melhor que o CLI atual (benchmark na Fase 3)
- ✅ Zero CNPJs duplicados entre execuções (garantia pelo banco)
- ✅ Operação 100% via navegador (nenhum acesso a terminal necessário)
- ✅ API 100% navegável via Swagger UI
- ✅ Deploy local com `docker compose up`

---

## 8. Próximos Passos Imediatos

1. **[Feito nesta entrega]** Registrar este plano no repositório.
2. Restaurar o `pom.xml` válido (Fase 0) — o arquivo atual está corrompido (contém texto do `.gitignore`).
3. Criar pasta `legacy/` e preservar o `Main.java` como referência de paridade.
4. Iniciar Fase 1 (refatoração do núcleo) assim que o build estiver verde.
