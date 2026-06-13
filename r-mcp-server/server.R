#!/usr/bin/env Rscript
# SDTM R MCP Server — entry point
# Uses the Posit mcp package: https://github.com/posit-dev/mcp
# Run:  Rscript server.R [--mode sse|stdio] [--port 8001]

suppressPackageStartupMessages({
  library(mcp)       # posit-dev/mcp
  library(jsonlite)
})

# Load tool implementations
source(file.path(dirname(sys.frame(1)$ofile), "tools", "core_tools.R"))

# Parse CLI args
args      <- commandArgs(trailingOnly = TRUE)
mode_flag <- grep("^--mode", args)
port_flag <- grep("^--port", args)
mode      <- if (length(mode_flag) && length(args) > mode_flag) args[mode_flag + 1] else "sse"
port      <- if (length(port_flag) && length(args) > port_flag) as.integer(args[port_flag + 1]) else 8001L

message(sprintf("[sdtm-mcp] Starting in %s mode on port %d", mode, port))

# ── Tool definitions ─────────────────────────────────────────────────────────
# Posit mcp API: tool(name, description, ..params.., .f = handler)
# Ref: https://github.com/posit-dev/mcp

srv <- mcp(
  tool(
    "health_check",
    "Return R server health status including version and package availability",
    .f = tool_health_check
  ),

  tool(
    "get_dag_status",
    "Return full DAG domain status for a session",
    session_id = type_string("Session identifier (format: YYYYMMDDHHmmss)"),
    .f = tool_get_dag_status
  ),

  tool(
    "get_dag_available",
    "Return list of domain codes currently available to process",
    session_id = type_string("Session identifier"),
    .f = tool_get_dag_available
  ),

  tool(
    "set_domain_status",
    "Update a domain's DAG status and propagate downstream unlocks",
    session_id = type_string("Session identifier"),
    domain     = type_string("SDTM domain code (e.g. DM, AE)"),
    status     = type_string("New status: pending|available|in_progress|complete|blocked"),
    .f = tool_set_domain_status
  ),

  tool(
    "check_dag_readiness",
    "Check whether all upstream dependencies for a domain are complete",
    session_id = type_string("Session identifier"),
    domain     = type_string("SDTM domain code to check"),
    .f = tool_check_dag_readiness
  )
)

# ── Start server ─────────────────────────────────────────────────────────────
if (mode == "stdio") {
  srv$run(transport = "stdio")
} else {
  srv$run(host = "0.0.0.0", port = port)
}
