#include "sql/stmt/drop_table_stmt.h"
#include "event/sql_debug.h"

RC DropTableStmt::create(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt)
{
  printf("Creating DropTableStmt in create function\n");
  stmt = new DropTableStmt(drop_table.relation_name);
  return RC::SUCCESS;
}