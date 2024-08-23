#include <iostream>
#include <map>
#include <stdint.h>
#include <string>
#include <vector>
#include <queue>
#include "taos.h"
#include <thread>         // std::thread
#include <mutex>          // std::mutex
#include <cstdio>
#include <experimental/filesystem>
#include <experimental/filesystem>
#include <fstream> 
#include<string.h>
#include <iomanip>
#include <unistd.h>


#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/filereadstream.h"

using namespace std;
using namespace rapidjson;

#define __SYNC_TAOS_DATA_FILE_ "./.syncTaosDataFile"
typedef int16_t  VarDataLenT;  // maxVarDataLen: 32767
#define varDataLen(v)       ((VarDataLenT *)(v))[0]
#define VARSTR_HEADER_SIZE  sizeof(VarDataLenT)
#define TSDB_NCHAR_SIZE         sizeof(unsigned short)

std::mutex FileMtx; 
std::mutex QueMtx;  

std::queue<std::string> tableNameQue;

bool queFlag = false;
double queSize = 0;

struct ParamInfo
{
    std::string column_name;
    std::string column_type;
};


typedef std::vector<ParamInfo> ParamVec;

typedef struct {
    std::string stable_name;
    ParamVec columns;
    ParamVec tags;
    std::string key;  // 主键字段
} StableInfo;

std::vector<StableInfo> stableInfos;
std::map<std::string, StableInfo> tableUsingStables; //key is table_name, value is stable_info


std::string getTableName(){

    std::string table_name = "";
    QueMtx.lock();
    if (!tableNameQue.empty())
    {
        table_name = tableNameQue.front();
        tableNameQue.pop();
    }

    int64_t now_size = tableNameQue.size();
    int count = (1- (now_size / queSize)) *100;
    string s = "\\|/-";
    char arry[101];
    string _sample="#";
    memset(arry,'\0',101); 
    for (size_t i = 0; i < count; i++)
    {
        arry[i] = _sample[0];
    }

    cout<<'['<<arry<<setw(100-count)<<']'<<" ["<<count<<"%"<<"]"<<" ["<<s[count%4]<<']'<<'\r'; //只回车不换行
    cout<<flush; //强制刷新缓冲区
    QueMtx.unlock();
    return table_name;


}

void putTableName(std::string table_name){

     if ("" == table_name)
        return;
    QueMtx.lock();
    tableNameQue.push(table_name);
    QueMtx.unlock();
}



void printUsage() {
    std::cout << "Usage: program -oh old_host -oP old_port -oU old_user -op old_password"
              << " -od old_db -ov old_version  -nh new_host -nP new_port -nU new_user -np new_password -nd new_db -nv new_version -st start_time -et end_time -th thread_num\n"
              << "       -oh  old_host 待同步数据库的ip \n"
              << "       -oP  old_port 待同步数据库的port 默认6030\n"
              << "       -oU  old_user 待同步数据库的用户 不填使用默认用户root\n"
              << "       -op  old_password 待同步数据库的密码 不填使用默认密码 \n"
              << "       -od  old_db 待同步数据库的数据库名 必填\n"
              << "       -oV  old_version 待同步数据库版本号 2.4/2.6版本填2.6， 3.0以上版本填3.0 必填\n"
              << "\n"
              << "       -nh  new_host 目标数据库的ip \n"
              << "       -nP  new_port 目标数据库的port 默认6030\n"
              << "       -nU  new_user 目标数据库的用户 不填使用默认用户root\n"
              << "       -np  new_password 目标数据库的密码 不填使用默认密码\n"
              << "       -nd  new_db 目标数据库的数据库名 必填\n"
              << "       -nV  new_version 目标数据库版本号 2.4/2.6版本填2.6， 3.0以上版本填3.0 必填\n"
              << "\n"
              << "       -st  start_time 开始时间时间戳，默认为 0 \n"
              << "       -et  end_time 结束时间时间戳，不设置为同步当前库的所有数据 \n"
              << "       -th  thread_num 线程数 默认为 5\n"
              << "       -co  commit_num 提交行数 默认为 1000\n"
              << std::endl;
}

void validateParams(std::map<std::string, std::string>& params, const std::vector<std::string>& requiredParams) {
    for (const auto& param : requiredParams) {
        if (params.find(param) == params.end()) {
            if (param == "-st" || param == "-et"){
               continue;
            }else if (param == "-oU") {
                params[param]="root";
            }else if (param == "-nU") {
                params[param]="root";
            }else if (param == "-op") {
                params[param]="taosdata";
            }else if (param == "-np") {
                params[param]="taosdata";
            }else if (param == "-th") {
                params[param]="5";
            }else if (param == "-oP") {
                params[param]="6030";
            }else if (param == "-nP") {
                params[param]="6030";
            }else if (param == "-co") {
                params[param]="1000";
            }else{
                throw std::invalid_argument("Missing required parameter: " + param);
            }
        }
    }
}

int  createFileIfNotExists() {
    FileMtx.lock();
    if (!std::experimental::filesystem::exists(__SYNC_TAOS_DATA_FILE_)) {
        std::ofstream outfile(__SYNC_TAOS_DATA_FILE_);
        if (!outfile.is_open()) {
            std::cerr << "Error creating file: " << __SYNC_TAOS_DATA_FILE_ << std::endl;
            
            FileMtx.unlock();
            return 1;
        }
        outfile.close();
    }
    FileMtx.unlock();
    return 0;
}

void appendToFile(const std::string& table_name , const std::string & timestamp) {
    
    FileMtx.lock();

   std::ifstream infile(__SYNC_TAOS_DATA_FILE_);
    if (!infile.is_open()) {
        std::cerr << "Error opening file: " << __SYNC_TAOS_DATA_FILE_ << std::endl;
        FileMtx.unlock();
        return;
    }

    std::vector<std::string> lines;
    std::string line;

    bool updateFlag = false;

    // 读取文件内容
    while (std::getline(infile, line)) {
        std::istringstream iss(line);
        std::string prefix, tp;
        if (!(iss >> prefix >> tp)) {
            std::cerr << "Error reading line: " << line << std::endl;
            continue;
        }
        // 如果匹配目标行，则更新时间戳
        if (prefix == table_name) {
            lines.push_back(table_name + " " + timestamp);
            updateFlag = true;
        } else {
            lines.push_back(prefix + " " + tp);
        }
    }
    if (!updateFlag)
    {
        lines.push_back(table_name + " " + timestamp);
    }

    infile.close();

    // 将更新后的内容写回文件
    std::ofstream outfile(__SYNC_TAOS_DATA_FILE_);
    if (!outfile.is_open()) {
        std::cerr << "Error opening file for writing: " << __SYNC_TAOS_DATA_FILE_ << std::endl;
        FileMtx.unlock();
        return;
    }

    for (const auto& updatedLine : lines) {
        outfile << updatedLine << std::endl;
    }

    outfile.close();

    FileMtx.unlock();
}

void getEndTimeFormFile(const std::string& table_name ,  std::string & timestamp) {
    
    FileMtx.lock();

   std::ifstream infile(__SYNC_TAOS_DATA_FILE_);
    if (!infile.is_open()) {
        std::cerr << "Error opening file: " << __SYNC_TAOS_DATA_FILE_ << std::endl;
            FileMtx.unlock();
        return;
    }

    std::vector<std::string> lines;
    std::string line;

    bool updateFlag = false;

    // 读取文件内容
    while (std::getline(infile, line)) {
        std::istringstream iss(line);
        std::string prefix, t_timestamp;
        if (!(iss >> prefix >> t_timestamp)) {
            std::cerr << "Error reading line: " << line << std::endl;
            continue;
        }
        // 如果匹配目标行，返回最后一次更新时间
        if (prefix == table_name) {
            timestamp = t_timestamp;
            FileMtx.unlock();
            return;
        }
    }
    infile.close();
    FileMtx.unlock();
}



int getAllTableName(std::map<std::string, std::string> params){
    
    TAOS * m_taos = taos_connect(params["-oh"].c_str(),params["-oU"].c_str(),params["-op"].c_str(),params["-od"].c_str(),std::atoi(params["-oP"].c_str()),params["-oV"].c_str());
   
    if (m_taos == nullptr)
    {
        std::cerr << "连接待同步数据库数据库失败"  << std::endl;
        return 1;
    }
    auto stableiter = stableInfos.begin();
    for (;stableiter != stableInfos.end();stableiter++)
    {
        std::string sql = "select  TBNAME from "+stableiter->stable_name +";";
        TAOS_RES *result =  taos_query(m_taos, sql.c_str(),params["-oV"].c_str());
        if (result == NULL || taos_errno(result,params["-oV"].c_str()) != 0){//判断查询是否成功
            std::cout << "数据库查询表失败："<< taos_errstr(result) << std::endl;
            taos_free_result(result);
            return 1;
        }

        TAOS_ROW    row;
        int         rows = 0;
        int         num_fields = taos_field_count(result,params["-oV"].c_str());
        TAOS_FIELD *fields = taos_fetch_fields(result,params["-oV"].c_str());

        while ((row = taos_fetch_row(result,params["-oV"].c_str()))) {
            std::string temp_name = "";
            int32_t charLen = varDataLen((char*)row[0] - VARSTR_HEADER_SIZE);
            bool copy_flag = false;
            if (fields[0].type == TSDB_DATA_TYPE_BINARY) {
                if(charLen <= fields[0].bytes && charLen >= 0){
                    copy_flag = true;
                }
            } else {
                if(charLen <= fields[0].bytes * TSDB_NCHAR_SIZE && charLen >= 0){
                    copy_flag = true;
                }
            }
            if(copy_flag){
                char temp_name_c[1024] = {0};
                memcpy(temp_name_c, row[0], charLen);
                temp_name = temp_name_c; 
                putTableName(temp_name);
                tableUsingStables[temp_name] = *stableiter;
            }
        }
        taos_free_result(result,params["-oV"].c_str());
    }

    
    queSize = tableNameQue.size();
    queFlag = true;
    return 0;
}


void transTask(std::map<std::string, std::string> params){
    void * o_taos = taos_connect(params["-oh"].c_str(),params["-oU"].c_str(),params["-op"].c_str(),params["-od"].c_str(),std::atoi(params["-oP"].c_str()),params["-oV"].c_str());
    if (o_taos == nullptr)
    {
        std::cerr << "Error connect taos 2.6 faile  " << std::endl;
        return;
    }
    
    void * n_taos = taos_connect(params["-nh"].c_str(),params["-nU"].c_str(),params["-np"].c_str(),params["-nd"].c_str(),std::atoi(params["-nP"].c_str()),params["-nV"].c_str());
    if (n_taos == nullptr)
    {
        std::cerr << "Error connect taos 3.0 faile  " << std::endl;
        return;
    }
    std::map<std::string, std::string> stable_query_col_str;
    std::map<std::string, std::string> stable_query_tag_str;

    auto stableiter = stableInfos.begin();
    for (;stableiter != stableInfos.end();stableiter++)
    {
        // 对数据库进行初始化操作
        auto columns_iter = stableiter->columns.begin();
        std::string create_column_sql = "";
        std::string query_col_sql = "";
        std::string query_tag_sql = "";
        int i = 0;
        for (;columns_iter != stableiter->columns.end();columns_iter++,i++)
        {
            if (i != 0)
            {
                create_column_sql += ",";
                query_col_sql += ",";
            }
            create_column_sql = create_column_sql + " `"+columns_iter->column_name + "` " + columns_iter->column_type ;
            query_col_sql =query_col_sql +" `"+columns_iter->column_name +"`";
        }
        if (query_col_sql.length() >0 )
        {
            stable_query_col_str.insert(std::make_pair(stableiter->stable_name,query_col_sql));
        }
        
        std::string tag_sql = "";
        auto tags_iter = stableiter->tags.begin();
        for (;tags_iter != stableiter->tags.end();tags_iter++)
        {
            tag_sql = tag_sql + " `"+tags_iter->column_name + "` " + tags_iter->column_type ;
            query_tag_sql =query_tag_sql +" `"+tags_iter->column_name +"`";

            if (tags_iter != stableiter->tags.end() - 1)
            {   
                tag_sql += ",";  
                query_tag_sql += ",";
            }
        }
        if (query_tag_sql.length() >0 )
        {
            stable_query_tag_str.insert(std::make_pair(stableiter->stable_name,query_tag_sql));
        }
        

        std::string  _create_stable_sql = "CREATE STABLE IF NOT EXISTS "+stableiter->stable_name + " ( "+create_column_sql+") "+ (tag_sql.length() > 0 ? " TAGS ("+tag_sql+")" : "") + " ;";

        TAOS_RES * res = taos_query(n_taos, _create_stable_sql.c_str(),params["-nV"].c_str());
        if (res == NULL || taos_errno(res,params["-nV"].c_str()) != 0) {
            std::cerr <<"failed to create stables , reason:" << taos_errstr(res,params["-nV"].c_str())<< std::endl;
            taos_free_result(res);
            taos_close(n_taos);
            return ;
        }
        taos_free_result(res,params["-nV"].c_str());
        res = NULL;
    }

    std::string table_name = "";
    std::string s_start_time = "";
    long long i_start_time = 0;
    std::string s_end_time = "";
    long long i_end_time = 0;
    auto st_iter = params.find("-st");
    if (st_iter != params.end())
    {
        s_start_time = st_iter->second;
    }
    
    auto et_iter = params.find("-et");
    if (et_iter != params.end())
    {
        s_end_time = et_iter->second;
    }

    if (s_start_time != "")
    {
       i_start_time = std::stoll(s_start_time);
    }
    if (s_end_time != "")
    {
       i_end_time = std::stoll(s_end_time);
    }

    bool getFileTimeFlag = true;

    while (queFlag && (table_name = getTableName()) != "")
    {
        
        std::string s_sync_end_time = ""; // 最后一次更新的时间
        long long i_sync_end_time = 0;// 最后一次更新的时间

        // 如果获取到了，直接将最后一次的时间同步的时间作为开始时间查询数据
       
        getEndTimeFormFile(table_name,s_sync_end_time);
        if (s_sync_end_time != "")
        {
            i_sync_end_time = std::stoll(s_sync_end_time);
        }
        
        auto stable_iter = tableUsingStables.find(table_name);
        if (stable_iter == tableUsingStables.end())
        {
            std::cout <<"table : " << table_name << " not has stable. " <<std::endl;
            continue;
        }
        auto query_col_iter = stable_query_col_str.find(stable_iter->second.stable_name);
        if (query_col_iter == stable_query_col_str.end())
        {
            std::cout <<"table : " << table_name << " not has query_col_str. " <<std::endl;
            continue;
        }
        auto query_tag_iter = stable_query_tag_str.find(stable_iter->second.stable_name);
        if (query_tag_iter == stable_query_tag_str.end())
        {
            std::cout <<"table : " << table_name << " not has query_tag_str. " <<std::endl;
            continue;
        }
        
        

        std::string query_time_begin = "";
        std::string query_time_end = "";

        if (i_sync_end_time != 0) // 已经同步过，并且时间不为0
        {
            if (i_start_time != 0 && i_start_time > i_sync_end_time) // 参数的开始时间不为0 并且开始时间大于已经结束的时间，以参数时间为开始时间
            {
                query_time_begin = " and `"+stable_iter->second.key+"` >= " + s_start_time;
            }else{ // 参数的开始时间为0或者开始时间小于已经同步的时间 ，以同步的时间为开始时间
                query_time_begin = " and `"+stable_iter->second.key+"` >= " +s_sync_end_time;
            } 
        }else if (i_start_time != 0) { // 未同步的数据，如果开始时间不为0 开始时间为参数的时间，参数时间未设置，则不设置开始时间
            query_time_begin = " and `"+stable_iter->second.key+"` >= " + s_start_time;
        }

        if (i_sync_end_time != 0) // 已经同步过，并且时间不为0
        {
            if (i_end_time != 0 && i_end_time <= i_sync_end_time) //如果参数中设置了结束时间，并且结束时间在已同步的时间之前，直接continue
            {
                continue;
            }
        }
        if (i_end_time != 0) { 
            query_time_end = " and `"+stable_iter->second.key+"` <= " + s_end_time;
        }

        std::string table_tag = "";

        if (query_tag_iter->second.length() > 0)
        {
            std::string old_select_tag = "select distinct "+query_tag_iter->second+" from " + table_name +";";
            std::string deviceId = "";
            std::string state = "";
            TAOS_RES *result =  taos_query(o_taos, old_select_tag.c_str(),params["-oV"].c_str());
            if (result == NULL || taos_errno(result,params["-oV"].c_str()) != 0){//判断查询是否成功
                std::cerr << "查询表标签列错误 ！ sql: " << old_select_tag << std::endl;
                taos_free_result(result,params["-oV"].c_str());
                return ;
            }

            TAOS_ROW    row;
            int         num_fields = taos_field_count(result,params["-oV"].c_str());
            TAOS_FIELD *fields = taos_fetch_fields(result,params["-oV"].c_str());

            while ((row = taos_fetch_row(result,params["-oV"].c_str()))) {

                for (size_t i = 0; i < num_fields; i++)
                {
                    if (i != 0)
                    {
                        table_tag += ",";
                    }
                    switch (fields[i].type) 
                    {
                    case TSDB_DATA_TYPE_BOOL:
                        table_tag += *((int8_t *)row[i] > 0 ? " TRUE " : " FALSE ");
                        break;
                    case TSDB_DATA_TYPE_TINYINT:
                        table_tag += std::to_string(*((int8_t *)row[i]));
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                        table_tag += std::to_string(*((int16_t *)row[i]));
                        break;    
                    case TSDB_DATA_TYPE_INT:
                        table_tag += std::to_string(*((int32_t *)row[i]));  
                        break;
                    case TSDB_DATA_TYPE_BIGINT: 
                        table_tag += std::to_string(*((int64_t *)row[i]));  
                        break;    
                    case TSDB_DATA_TYPE_FLOAT:  
                        table_tag += std::to_string(*((float  *)row[i]));   
                        break;    
                    case TSDB_DATA_TYPE_DOUBLE:  
                        table_tag += std::to_string(*((double  *)row[i]));   
                        break;    
                    case TSDB_DATA_TYPE_NCHAR:  
                        table_tag += "\""+std::string((char *)row[i])+"\""; 
                        break;    
                    case TSDB_DATA_TYPE_BINARY:  
                        table_tag += "\""+std::string((char *)row[i])+"\""; 
                        break;    
                    case TSDB_DATA_TYPE_TIMESTAMP:  
                        table_tag += std::to_string(*((int64_t  *)row[i]));   
                        break;      
                    case TSDB_DATA_TYPE_BLOB:  
                        table_tag += "\""+std::string((char *)row[i])+"\"";   
                        break;    
                    case TSDB_DATA_TYPE_JSON:  
                        table_tag += "\""+std::string((char *)row[i])+"\"";     
                        break;    
                    case TSDB_DATA_TYPE_GEOMETRY:  
                        table_tag += "\""+std::string((char *)row[i])+"\"";      
                        break;     
                    default:
                        std::cout << table_name <<  " unknown data type " << std::endl;
                        return ;
                    }
                }
                
            }
            taos_free_result(result,params["-oV"].c_str());
            result = NULL;
        }
        

        

        // 新数据库建表
        std::string _create_sql = "create table if not exists " + table_name +" USING "+stable_iter->second.stable_name+ (table_tag.length() > 0 ? " Tags ("+table_tag+");":";");
        TAOS_RES *result =  taos_query(n_taos, _create_sql.c_str(),params["-nV"].c_str());
	    if (result == NULL || taos_errno(result,params["-nV"].c_str()) != 0){//判断查询是否成功
	        std::cerr << "新数据库建表错误 ！ sql: " << _create_sql <<"  reason:" << taos_errstr(result,params["-nV"].c_str()) << std::endl;
	        taos_free_result(result);
	        return ;
	    }

        // 查询老的表数据
        std::string old_select_sql = "select "+query_col_iter->second+" from "+table_name + " where _c0 > 0 "+ query_time_begin +query_time_end +";";

        result =  taos_query(o_taos, old_select_sql.c_str(),params["-oV"].c_str());
	    if (result == NULL || taos_errno(result,params["-oV"].c_str()) != 0){//判断查询是否成功
	        std::cerr << "查询老历史数据错误 ！ sql: " << old_select_sql <<"  reason:" << taos_errstr(result,params["-oV"].c_str()) << std::endl;
	        taos_free_result(result,params["-oV"].c_str());
	        return ;
	    }
        
        std::string new_insert_sql_begin = "insert into "+table_name +" values ";

        std::string new_insert_sql_middle = "";
        std::string new_insert_sql_end = ";";
        
        int i = 0;
        int64_t last_time = 0;

        TAOS_ROW    row;
        int num_fields = taos_field_count(result,params["-oV"].c_str());
		TAOS_FIELD *fields = taos_fetch_fields(result,params["-oV"].c_str());

        
	    while ((row = taos_fetch_row(result,params["-oV"].c_str()))) {
            i++;
	        
	        int64_t time = *((int64_t *)row[0]); 
            new_insert_sql_middle = new_insert_sql_middle ;
            std::string tmp_sql_middle = "(";
            for (size_t j = 0; j < num_fields; j++)
            {
                if (j != 0)
                {
                    tmp_sql_middle += ",";
                }
                switch (fields[j].type) 
                {
                case TSDB_DATA_TYPE_BOOL:
                    tmp_sql_middle += *((int8_t *)row[j] > 0 ? " TRUE " : " FALSE ");
                    break;
                case TSDB_DATA_TYPE_TINYINT:
                    tmp_sql_middle += std::to_string(*((int8_t *)row[j]));
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                    tmp_sql_middle += std::to_string(*((int16_t *)row[j]));
                    break;    
                case TSDB_DATA_TYPE_INT:
                    tmp_sql_middle += std::to_string(*((int32_t *)row[j]));  
                    break;
                case TSDB_DATA_TYPE_BIGINT: 
                    tmp_sql_middle += std::to_string(*((int64_t *)row[j]));  
                    break;    
                case TSDB_DATA_TYPE_FLOAT:  
                    tmp_sql_middle += std::to_string(*((float  *)row[j]));   
                    break;    
                case TSDB_DATA_TYPE_DOUBLE:  
                    tmp_sql_middle += std::to_string(*((double  *)row[j]));   
                    break;    
                case TSDB_DATA_TYPE_NCHAR:  
                    tmp_sql_middle += (char *)row[j];   
                    break;    
                case TSDB_DATA_TYPE_BINARY:  
                    tmp_sql_middle += (char *)row[j];   
                    break;    
                case TSDB_DATA_TYPE_TIMESTAMP:  
                    tmp_sql_middle += std::to_string(*((int64_t  *)row[j]));   
                    break;      
                case TSDB_DATA_TYPE_BLOB:  
                    tmp_sql_middle += std::to_string(*((int64_t  *)row[j]));   
                    break;    
                case TSDB_DATA_TYPE_JSON:  
                    tmp_sql_middle += std::to_string(*((int64_t  *)row[j]));     
                    break;    
                case TSDB_DATA_TYPE_GEOMETRY:  
                    tmp_sql_middle += std::to_string(*((int64_t  *)row[j]));       
                    break;     
                default:
                    std::cout << table_name <<  " unknown data type " << std::endl;
                    tmp_sql_middle = "";
                    return ;
                }
            }

            tmp_sql_middle += ")";
            new_insert_sql_middle += tmp_sql_middle;
         
            if (i % std::stoi(params["-co"]) == 0)
            {
                std::string insert_sql = new_insert_sql_begin+new_insert_sql_middle+new_insert_sql_end;
                TAOS_RES * insert_res =  taos_query(n_taos, insert_sql.c_str(),params["-nV"].c_str());
                if (insert_res == NULL || taos_errno(insert_res,params["-nV"].c_str()) != 0){//判断查询是否成功
                    std::cerr << "新数据库插入错误 ！ sql: " << _create_sql <<"  reason:" << taos_errstr(insert_res,params["-nV"].c_str()) << std::endl;
                    taos_free_result(insert_res,params["-nV"].c_str());
                    return ;
                }
                appendToFile(table_name,std::to_string(time));
                taos_free_result(insert_res,params["-nV"].c_str());
                new_insert_sql_middle = "";
                i=0;
            }
            last_time = time;
        }
        
        if (!new_insert_sql_middle.empty())
        {
            std::string insert_sql = new_insert_sql_begin+new_insert_sql_middle+new_insert_sql_end;
            TAOS_RES * insert_res =  taos_query(n_taos, insert_sql.c_str(),params["-nV"].c_str());
            if (insert_res == NULL || taos_errno(insert_res,params["-nV"].c_str()) != 0){//判断查询是否成功
                std::cerr << "新数据库插入错误 ！ sql: " << _create_sql <<"  reason:" << taos_errstr(insert_res,params["-nV"].c_str()) << std::endl;
                taos_free_result(insert_res,params["-nV"].c_str());
                return ;
            }
            appendToFile(table_name,std::to_string(last_time));
            taos_free_result(insert_res,params["-nV"].c_str());
            new_insert_sql_middle = "";
            i=0;
        }


    }
    return;
}


int parseJsonFile(){
    std::ifstream ifs("stable_define.json");
    if (!ifs.is_open())
    {   
        std::cerr << "open stable_define.json file error" << std::endl;
        return 1;
    }
    // 读取文件内容到字符串
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    std::string jsonContent = buffer.str();

     // 解析 JSON
    rapidjson::Document document;
    document.Parse(jsonContent.c_str());

    // 检查 JSON 是否是数组
    if (document.IsArray()) {
        const rapidjson::Value& array = document.GetArray();

        for (rapidjson::SizeType i = 0; i < array.Size(); i++) {
            StableInfo stableinfo;
            const rapidjson::Value& obj = array[i];
            // 读取 stableName
            if (obj.HasMember("stableName") && obj["stableName"].IsString()) {
                stableinfo.stable_name = obj["stableName"].GetString();
            }

            // 读取 columns
            if (obj.HasMember("columns") && obj["columns"].IsArray()) {
                const rapidjson::Value& columns = obj["columns"].GetArray();
                ParamVec paramsVec;
                for (rapidjson::SizeType j = 0; j < columns.Size(); j++) {
                    const rapidjson::Value& column = columns[j];
                    std::string columnName = "";
                    std::string columnType = "";
                    if (column.HasMember("columnName") && column["columnName"].IsString()) {
                        columnName = column["columnName"].GetString();
                    }
                    if (column.HasMember("columnType") && column["columnType"].IsString()) {
                       columnType = column["columnType"].GetString();
                    }
                    if (columnName != "" && columnType != "")
                    {
                        ParamInfo paraminfo;
                        paraminfo.column_name = columnName;
                        paraminfo.column_type = columnType;
                        paramsVec.push_back(paraminfo);
                        if (j == 0)
                        {
                            stableinfo.key = columnName;
                        }
                    }
                    
                }
                stableinfo.columns = paramsVec;
            }
            // 读取 tags
            if (obj.HasMember("tags") && obj["tags"].IsArray()) {
                const rapidjson::Value& tags = obj["tags"].GetArray();
                ParamVec tagsVec;
                for (rapidjson::SizeType k = 0; k < tags.Size(); k++) {
                    std::string tagName = "";
                    std::string tagType = "";
                    const rapidjson::Value& tag = tags[k];
                    if (tag.HasMember("tagName") && tag["tagName"].IsString()) {
                        tagName = tag["tagName"].GetString();
                    }
                    if (tag.HasMember("tagType") && tag["tagType"].IsString()) {
                        tagType = tag["tagType"].GetString();
                    }
                    if (tagName != "" && tagType != "")
                    {
                        ParamInfo paraminfo;
                        paraminfo.column_name = tagName;
                        paraminfo.column_type = tagType;
                        tagsVec.push_back(paraminfo);
                    }
                }
                stableinfo.tags = tagsVec;
            }
            if (!stableinfo.stable_name.empty() &&!stableinfo.columns.empty())
            {
                stableInfos.push_back(stableinfo);
            }
        }
    }
    else {
        std::cerr << "stable_define.json is not an array" << std::endl;
        return 1;
    }

    return 0;
}


int main(int argc, char* argv[]) {
    std::vector<std::string> requiredParams = {"-oh", "-nh", "-oP", "-nP", "-nU", "-oU","-oV","-nV", "-np", "-op", "-nd", "-od","-st","-et","-th","-co"};


    if (argc < 9) {
        printUsage();
        return 1;
    }

    std::map<std::string, std::string> params;
    try {
        for (int i = 1; i < argc; i += 2) {
            std::string key = argv[i];
            std::string value = argv[i + 1];
            if (key[0] != '-') {
                throw std::invalid_argument("Invalid parameter key: " + key);
            }
            params[key] = value;
        }

        validateParams(params, requiredParams);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        printUsage();
        return 1;
    }

    int t_ret = taos_init(params["-nV"].c_str());
    if (t_ret != 0)
	{
		std::cout << "##### taos_init error #####";
		return 1;
	}
    if (createFileIfNotExists() != 0)
    {
        return 1;
    }

    // 解析json，获取需要同步的数据库中的所有超表信息

    if (parseJsonFile() != 0){
        return 1;
    }



    // 获取待同步数据库的所有表名称并push到队列中

    if (getAllTableName(params) != 0){
        return 1;
    }


    int _th_num = 5;
    auto _th_iter = params.find("-th");
    if (_th_iter != params.end())
    {
        _th_num = std::atoi(_th_iter->second.c_str());
    }

     std::vector<std::thread> threads;
    // 创建并启动多个线程
    for (int i = 0; i < _th_num; ++i) {
        threads.emplace_back(transTask, params);
    }

    // 等待所有线程完成
    for (auto& th : threads) {
        if (th.joinable()) {
            th.join();
        }
    }
    //正常结束，删除中间文件
    std::remove(__SYNC_TAOS_DATA_FILE_);

    taos_cleanup(params["-nV"].c_str());

    return 0;
}