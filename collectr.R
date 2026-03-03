if (!require("f1dataR")) {
  install.packages("f1dataR")
}


if (!dir.exists("data")) {
  dir.create("data")
  message("Pasta 'data' criada com sucesso!")
}

library(optparse)
library(R6)
library(f1dataR) # Substitui o reticulate
library(arrow)
library(glue)

Collect <- R6Class(
  "Collect",
  public = list(
    years = NULL,
    modes = NULL,

    # initialize simplificado (não precisamos mais importar o fastf1 do Python)
    initialize = function(
      years = c(2021, 2022, 2023),
      modes = c("R", "S")
    ) {
      self$years <- years
      self$modes <- modes

      # Opcional, mas recomendado: Ativa o cache do f1dataR na sua pasta de projeto
      # f1dataR::change_cache(file.path(getwd(), "f1_cache"))
    },

    # O método get_data agora usa as funções nativas do f1dataR
    get_data = function(year, gp, mode) {
      df <- tryCatch(
        {
          # No f1dataR, as funções variam de acordo com a sessão
          if (mode == "R") {
            f1dataR::load_results(season = year, round = gp)
          } else if (mode == "S") {
            f1dataR::load_sprint(season = year, round = gp)
          } else if (mode == "Q") {
            f1dataR::load_quali(season = year, round = gp)
          } else {
            message("Modo de sessão não suportado nesta função.")
            return(NULL)
          }
        },
        error = function(e) {
          return(NULL) # Retorna NULL se o GP ou a Sprint não existir
        }
      )

      return(df)
    },

    save_data = function(df, year, gp, mode) {
      gp_id <- formatC(as.integer(gp), width = 2, flag = "0")
      file_path <- glue::glue("data/{year}_{gp_id}_{mode}.parquet")

      if (!dir.exists("data")) {
        dir.create("data")
      }

      arrow::write_parquet(df, file_path)
      message(glue::glue("Arquivo salvo: {file_path}"))
    },

    process = function(year, gp, mode) {
      df <- self$get_data(year, gp, mode)

      if (is.null(df) || nrow(df) == 0) {
        # Opcional: Silenciar a mensagem para não poluir o console quando for apenas procurar o fim da temporada
        # message(glue::glue("Nenhum dado encontrado para {year} GP {gp} [{mode}]"))
        return(FALSE)
      }

      self$save_data(df, year, gp, mode)
      return(TRUE)
    },

    process_year_mode = function(year) {
      for (i in 1:49) {
        for (mode in self$modes) {
          success <- self$process(year, i, mode)

          if (!success && mode == "R") {
            message(glue::glue("Fim da temporada {year} detectado no GP {i}."))
            return(NULL)
          }

          if (!success && mode == "S") {
            message(glue::glue("GP {i} não teve Sprint. Tentando Corrida..."))
          }
        }
      }
    },

    process_years = function() {
      for (year in self$years) {
        message(glue::glue(
          "\n========== INICIANDO TEMPORADA {year} ==========\n"
        ))

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

coletor <- Collect$new(years = years_vec, modes = modes_vec)
coletor$process_years()

# teste
coletor <- Collect$new(years = c(2022), modes = c("R"))
coletor$process_years()
