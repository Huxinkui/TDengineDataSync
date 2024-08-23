#ifndef _TAOS_COMMON_H_
#define _TAOS_COMMON_H_
#include <iostream>
#include <string>

#if defined (WIN32) | defined (WIN64)
	#include <windows.h>  // 在Windows中使用
#else
	#include <dlfcn.h>  // 在Linux/Unix中使用
#endif


typedef void   TAOS;
typedef void   TAOS_STMT;
typedef void   TAOS_RES;
typedef void   TAOS_STREAM;
typedef void   TAOS_SUB;
typedef void **TAOS_ROW;

// Data type definition
#define TSDB_DATA_TYPE_NULL       0   // 1 bytes
#define TSDB_DATA_TYPE_BOOL       1   // 1 bytes
#define TSDB_DATA_TYPE_TINYINT    2   // 1 byte
#define TSDB_DATA_TYPE_SMALLINT   3   // 2 bytes
#define TSDB_DATA_TYPE_INT        4   // 4 bytes
#define TSDB_DATA_TYPE_BIGINT     5   // 8 bytes
#define TSDB_DATA_TYPE_FLOAT      6   // 4 bytes
#define TSDB_DATA_TYPE_DOUBLE     7   // 8 bytes
#define TSDB_DATA_TYPE_VARCHAR    8   // string, alias for varchar
#define TSDB_DATA_TYPE_TIMESTAMP  9   // 8 bytes
#define TSDB_DATA_TYPE_NCHAR      10  // unicode string
#define TSDB_DATA_TYPE_UTINYINT   11  // 1 byte
#define TSDB_DATA_TYPE_USMALLINT  12  // 2 bytes
#define TSDB_DATA_TYPE_UINT       13  // 4 bytes
#define TSDB_DATA_TYPE_UBIGINT    14  // 8 bytes
#define TSDB_DATA_TYPE_JSON       15  // json string
#define TSDB_DATA_TYPE_VARBINARY  16  // binary
#define TSDB_DATA_TYPE_DECIMAL    17  // decimal
#define TSDB_DATA_TYPE_BLOB       18  // binary
#define TSDB_DATA_TYPE_MEDIUMBLOB 19
#define TSDB_DATA_TYPE_BINARY     TSDB_DATA_TYPE_VARCHAR  // string
#define TSDB_DATA_TYPE_GEOMETRY   20  // geometry
#define TSDB_DATA_TYPE_MAX        21


typedef struct taosField {
  char     name[65];
  uint8_t  type;
  uint16_t bytes;
} TAOS_FIELD;

// 替换taos动态库函数 
int taos_init(std::string version = "2.6");
void taos_cleanup(std::string version = "2.6");
TAOS * taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port,std::string version = "2.6");
TAOS_RES * taos_query(TAOS *taos, const char *sqlstr,std::string version = "2.6");  
int taos_errno(TAOS_RES *tres,std::string version = "2.6");
char* taos_errstr(TAOS_RES *tres,std::string version = "2.6");
void taos_free_result(TAOS_RES *tres,std::string version = "2.6");
void taos_close(TAOS *taos,std::string version = "2.6");
int taos_field_count(TAOS_RES *res,std::string version = "2.6");
TAOS_FIELD *taos_fetch_fields(TAOS_RES *res,std::string version = "2.6");
TAOS_ROW taos_fetch_row(TAOS_RES *res,std::string version = "2.6");

#endif
