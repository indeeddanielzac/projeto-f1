# --- Verificação e Instalação Automática de Dependências ---
packages <- c("optparse", "R6", "reticulate", "glue", "tzdb", "arrow")

# Identifica quais pacotes da lista NÃO estão instalados
missing_packages <- packages[!(packages %in% installed.packages()[, "Package"])]

# Se houver pacotes faltando, instala-os
if (length(missing_packages)) {
    message(
        "Instalando pacotes faltantes: ",
        paste(missing_packages, collapse = ", ")
    )
    install.packages(missing_packages, repos = "https://cloud.r-project.org")
}

library(optparse)
library(R6)
library(reticulate)
library(glue)
library(arrow)


# virtualenv_create("r-fastf1")

# virtualenv_install("r-fastf1", "fastf1")

reticulate::use_virtualenv("r-fastf1", required = TRUE)

ff1 <- import("fastf1")

# Certifique-se que o ambiente está ativo
use_virtualenv("r-fastf1", required = TRUE)

ff1 <- import("fastf1")

if (!dir.exists("data")) {
    dir.create("data")
    message("Pasta 'data' criada com sucesso!")
}


Collect <- R6Class(
    "Collect",
    public = list(
        years = NULL,
        modes = NULL,
        ff1 = NULL,

        # O __init__ do Python no R6 é o método initialize
        initialize = function(
            years = c(2021, 2022, 2023),
            modes = c("R", "S")
        ) {
            self$years <- years
            self$modes <- modes
            # Importa o fastf1 para dentro da classe
            self$ff1 <- import("fastf1")
        },

        # O método get_data
        get_data = function(year, gp, mode) {
            # tryCatch evita que o script morra se o FastF1 der erro
            df <- tryCatch(
                {
                    session <- self$ff1$get_session(
                        as.integer(year),
                        as.integer(gp),
                        mode
                    )
                    session$`_load_drivers_results`()
                    as.data.frame(session$results)
                },
                error = function(e) {
                    return(NULL) # Se der erro no Python, retorna NULL para o R
                }
            )

            return(df)
        },

        save_data = function(df, year, gp, mode) {
            # Cria o nome do arquivo com zero à esquerda no GP
            gp_id <- formatC(as.integer(gp), width = 2, flag = "0")
            file_path <- glue::glue("data/{year}_{gp_id}_{mode}.parquet")

            # Garante que a pasta existe
            if (!dir.exists("data")) {
                dir.create("data")
            }

            # Salva usando a função do arrow (R nativo) ou o método do Python
            # O jeito mais seguro no R:
            arrow::write_parquet(df, file_path)

            message(glue::glue("Arquivo salvo: {file_path}"))
        },

        # Método para processar a coleta
        process = function(year, gp, mode) {
            # Busca os dados usando o método da própria classe
            df <- self$get_data(year, gp, mode)

            # Verifica se o dataframe está vazio
            if (is.null(df) || nrow(df) == 0) {
                message(glue::glue(
                    "Nenhum dado encontrado para {year} GP {gp}"
                ))
                return(FALSE)
            }

            # Chama o método de salvar
            self$save_data(df, year, gp, mode)
            return(TRUE)
        },

        process_year_mode = function(year) {
            # No R, range(1, 50) é 1:49
            for (i in 1:49) {
                # Percorre os modos (S, R) definidos no self$modes
                for (mode in self$modes) {
                    # Tenta processar o GP 'i' no modo 'mode'
                    success <- self$process(year, i, mode)

                    # Lógica: Se a CORRIDA (R) falhar, assume que a temporada acabou
                    # No R, 'and' é '&&'
                    if (!success && mode == "R") {
                        message(glue::glue(
                            "Fim da temporada {year} detectado no GP {i}."
                        ))
                        return(NULL) # Sai da função process_year_mode
                    }

                    # Se a SPRINT (S) falhar, o loop do 'mode' continua para tentar a 'R'
                    if (!success && mode == "S") {
                        message(glue::glue(
                            "GP {i} não teve Sprint. Tentando Corrida..."
                        ))
                    }
                }
            }
        },

        process_years = function() {
            # No R, iteramos sobre o vetor self$years
            for (year in self$years) {
                message(glue::glue(
                    "\n========== INICIANDO TEMPORADA {year} ==========\n"
                ))
                # Chama o método que percorre os GPs e Modos
                self$process_year_mode(year)

                message(glue::glue(
                    "\n========== TEMPORADA {year} CONCLUÍDA ==========\n"
                ))
                Sys.sleep(10)
            }

            message("### Coleta total finalizada com sucesso! ###")
        }
    )
)

#arquivos <- list.files("data", full.names = TRUE)
#file.remove(arquivos)

# --- Configuração do Parser de Argumentos ---
option_list <- list(
    make_option(
        c("-y", "--years"),
        type = "character",
        default = "2023",
        help = "Anos separados por vírgula [default %default]",
        metavar = "number"
    ),
    make_option(
        c("-m", "--modes"),
        type = "character",
        default = "R,S",
        help = "Modos separados por vírgula (R, S) [default %default]",
        metavar = "character"
    )
)

opt_parser <- OptionParser(option_list = option_list)
args <- parse_args(opt_parser)

# Tratamento dos inputs (converte string "2021,2022" em vetor c(2021, 2022))
years_vec <- as.integer(unlist(strsplit(args$years, ",")))
modes_vec <- unlist(strsplit(args$modes, ","))

# --- Como usar ---
#Cria a instância (igual ao Python)
#collect <- Collect$new(years = as.integer(c(2023, 2024, 2025)), modes = c("S", "R"))

collect <- Collect$new(years = years_vec, modes = modes_vec)

collect$process_years()

# Para rodar anos específicos:
# R.home("bin")
# & "[Adicionar caminho].Rscript.exe" collect.R --years 2023,2024,2025 --modes R,S

# Para rodar apenas um ano e apenas Corridas:
# Rscript collect.R -y 2024 -m R
