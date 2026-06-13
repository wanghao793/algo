library(jsonlite)

dag_schema_path <- file.path(dirname(dirname(sys.frame(1)$ofile)), "dag", "dag_schema.json")

read_dag_schema <- function() {
  fromJSON(dag_schema_path, simplifyDataFrame = FALSE)
}

# In-memory session DAG state: list keyed by session_id
# Each entry: named list of domain -> status
.session_dag <- new.env(parent = emptyenv())

get_or_init_session_dag <- function(session_id) {
  if (is.null(.session_dag[[session_id]])) {
    schema <- read_dag_schema()
    statuses <- setNames(
      lapply(schema$domains, function(d) {
        if (length(d$depends_on) == 0) "available" else "pending"
      }),
      sapply(schema$domains, `[[`, "domain")
    )
    .session_dag[[session_id]] <- statuses
  }
  .session_dag[[session_id]]
}

# Recompute which domains are available after a status change
propagate_dag <- function(session_id) {
  schema   <- read_dag_schema()
  statuses <- .session_dag[[session_id]]

  for (d in schema$domains) {
    dom <- d$domain
    if (statuses[[dom]] %in% c("pending", "blocked")) {
      deps_done <- all(sapply(d$depends_on, function(dep) {
        identical(statuses[[dep]], "complete")
      }))
      if (deps_done) statuses[[dom]] <- "available"
    }
  }
  .session_dag[[session_id]] <- statuses
  statuses
}

# ── Tool implementations ────────────────────────────────────────────────────

tool_health_check <- function() {
  list(
    status      = "ok",
    r_version   = R.version.string,
    sdtm_oak    = tryCatch(as.character(packageVersion("sdtm.oak")), error = function(e) "not installed"),
    timestamp_utc = format(Sys.time(), tz = "UTC", "%Y-%m-%dT%H:%M:%SZ")
  )
}

tool_get_dag_status <- function(session_id) {
  statuses <- get_or_init_session_dag(session_id)
  schema   <- read_dag_schema()
  lapply(schema$domains, function(d) {
    list(
      domain     = d$domain,
      full_name  = d$full_name,
      class      = d$class,
      layer      = d$layer,
      depends_on = d$depends_on,
      status     = statuses[[d$domain]]
    )
  })
}

tool_get_dag_available <- function(session_id) {
  statuses <- get_or_init_session_dag(session_id)
  Filter(function(d) identical(statuses[[d]], "available"), names(statuses))
}

tool_set_domain_status <- function(session_id, domain, status) {
  allowed <- c("pending", "available", "in_progress", "complete", "blocked")
  if (!status %in% allowed) stop(paste("Invalid status:", status))

  statuses <- get_or_init_session_dag(session_id)
  if (is.null(statuses[[domain]])) stop(paste("Unknown domain:", domain))

  statuses[[domain]] <- status
  .session_dag[[session_id]] <- statuses
  propagate_dag(session_id)

  list(domain = domain, status = status, session_id = session_id)
}

tool_check_dag_readiness <- function(session_id, domain) {
  statuses <- get_or_init_session_dag(session_id)
  schema   <- read_dag_schema()
  d_def    <- Filter(function(d) d$domain == domain, schema$domains)[[1]]

  missing_deps <- Filter(function(dep) !identical(statuses[[dep]], "complete"), d_def$depends_on)
  list(
    domain    = domain,
    ready     = length(missing_deps) == 0,
    missing   = missing_deps
  )
}
